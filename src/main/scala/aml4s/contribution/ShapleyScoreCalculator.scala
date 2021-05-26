package aml4s.contribution

import aml4s.model.Models.SupervisedModel
import breeze.linalg._
import breeze.stats.distributions.RandBasis
import org.apache.spark.ml.Model
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object ShapleyScoreCalculator {
  case class ShapleyFeature(id: Long, features: Seq[(String, Double)], weight: Double)
}

case class ShapleyScoreCalculator(implicit spark: SparkSession) {

  import ShapleyScoreCalculator._

  def computePermutations(partitionIndex: Int, features: Seq[String]): Seq[String] = {
    RandBasis
      .withSeed(partitionIndex)
      .permutation(features.size)
      .draw()
      .map(idx => features(idx))
  }

  def computeFeatureVectorRows(
                                partitionIndex: Int,
                                idCol: String,
                                randRows: Iterator[Row],
                                rowToInvestigate: Row,
                                features: Seq[String],
                                weightColName: Option[String] = None
                              ): Iterator[ShapleyFeature] = {

    /* We cycle through permutations in cases where the number of samples is more than
     the number of features */

    val featurePermutations = computePermutations(partitionIndex, features)

    val permutationIter = Iterator.continually(featurePermutations.permutations).flatten

    val featureVectorRows = collection.mutable.Buffer.empty[ShapleyFeature]

    randRows.foreach { randRow =>
      /* take sample z from training set */
      val weight = if (weightColName.isDefined) randRow.getAs(weightColName.get) else 1d
      val featurePermutation = permutationIter.next()
      /* gather: {z_1, ..., z_p} */

      var featureVectorWithoutFeature =
        ShapleyFeature(
          randRow.getAs[Int](idCol),
          features.map(feature => (feature, randRow.getAs[Double](feature))),
          weight
        )

      var featureVectorWithFeature = featureVectorWithoutFeature

      featurePermutation.foreach { featureName =>
        /* for random feature k */
        /* x_+k = {x_1, ..., x_k, .. z_p} */
        featureVectorWithFeature = featureVectorWithFeature.copy(features =
          featureVectorWithFeature.features :+ (
            (
              featureName,
              rowToInvestigate.getAs[Double](featureName)
            )
            )
        )
        /* x_-k = {x_1, ..., x_{k-1}, z_k, ..., z_p} */
        /* store (x_+k, x_-k) */
        featureVectorRows += featureVectorWithFeature
        featureVectorRows += featureVectorWithoutFeature
        /* (x_-k = x_+k) */
        featureVectorWithoutFeature = featureVectorWithoutFeature.copy(features =
          featureVectorWithoutFeature.features :+ (
            (
              featureName,
              rowToInvestigate.getAs[Double](featureName)
            )
            )
        )
      }
    }

    featureVectorRows.toIterator

  }

  /** Computes the shapley marginal contribution for each feature in the feature vector over all
    * samples in the partition. The algorithm is based on a monte-carlo approximation:
    * https://christophm.github.io/interpretable-ml-book/shapley.html#fn42
    *
    * : Index of spark partition which will serve as a seed to numpy
    *
    * @return
    * Map of feature -> tuple of shapley marginal contribution and feature weight
    */
  def computeShapleyScore[M <: Model[M]](
                                          featureVectorRows: DataFrame,
                                          model: SupervisedModel[M],
                                          featureNames: Seq[String]
                                        ): DataFrame = {

    import spark.implicits._

    val weights = featureVectorRows.rdd.map(_.getAs[Double]("weight")).collect()

    val explodedFeatures = featureVectorRows
      .withColumn("features", F.expr("explode(features)"))
      .withColumn("feature", F.expr("features._1"))
      .withColumn("value", F.expr("features._2"))
      .groupBy("id")
      .pivot("feature")
      .agg(F.first("value"))
      .join(featureVectorRows, usingColumn = "id")
      .drop("features")

    val vectorAssembler = new VectorAssembler()
      .setInputCols(featureNames.toArray)
      .setOutputCol("features")

    val assembledFeatures = vectorAssembler.transform(explodedFeatures)

    val predictions = model.predictionsWithProba(assembledFeatures)
    val predictProba = predictions.drop("prediction", "features")

    predictProba.rdd.mapPartitionsWithIndex { (idx, preds) =>

      val featureIterator =
        Iterator.continually(computePermutations(idx, featureNames).permutations).flatten.flatten

      (for {
        idx <- (0 until preds.size by 2)
        pred <- preds
        nextPred = preds.next()
        feature = featureIterator.next()
      } yield {
        val marginalContribution = pred.getAs[Double]("probability") - nextPred.getAs[Double]("probability")
        /* There is one weight added per random row visited.
           For each random row visit, we generate 2 predictions for each required feature.
           Therefore, to get index into rand_row_weights, we need to divide
           prediction index by 2 * number of features, and take the floor of this
         */
        val weight = weights(idx / featureNames.size)
        (feature, marginalContribution, weight)
      }).toIterator
    }.toDF("feature", "marginal_contribution", "weight")

  }

  /** Compute shapley values for all features in a given feature vector of interest
    *
    * @param df
    * : Training dataset
    * @param model
    * a Model object which implements the predictProba function.
    * @param rowToInvestigate
    * : Feature vector for which we need to compute shapley scores
    * @param weightColName
    * : column name with row weights to use when sampling the training set
    * @return
    * Map of feature mapping to its corresponding shapley value.
    */

  def computeShapleyForSample[M <: Model[M]](
                                              df: DataFrame,
                                              idCol: String,
                                              model: SupervisedModel[M],
                                              rowToInvestigate: Row,
                                              features: Seq[String],
                                              weightColName: Option[String] = None
                                            ): Map[String, Double] = {

    import spark.implicits._

    val featureVectorRows = df.rdd
      .mapPartitionsWithIndex(
        { (idx, rows) =>
          computeFeatureVectorRows(
            idx,
            idCol,
            rows,
            rowToInvestigate,
            features,
            weightColName
          )
        },
        preservesPartitioning = true
      )
      .toDF()

    val shapleyDf = computeShapleyScore(featureVectorRows, model, features)

    shapleyDf.groupBy("feature").agg(

      (
        F.sum(shapleyDf.col("marginal_contribution") * shapleyDf.col("weight")) /
          F.sum(shapleyDf.col("weight"))
        ).alias("shapley_value"))
      .collect()
      .map { row =>
        (row.getAs[String]("feature"), row.getAs[Double]("shapley_value"))
      }.toMap

  }


}
