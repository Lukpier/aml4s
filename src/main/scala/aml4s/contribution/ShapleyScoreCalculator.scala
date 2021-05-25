package aml4s.contribution

import aml4s.model.Models.SupervisedModel
import breeze.linalg._
import breeze.stats.distributions.RandBasis
import org.apache.spark.ml.Model
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.{expr, first}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
case class ShapleyFeature(id: Long, features: Seq[(String, Double)], weight: Double)

case class ShapleyScore(feature: String, contribution: Double, weight: Double)

case class ShapleyScoreCalculator(implicit spark: SparkSession) {

  def analyze[M <: Model[M]](
      df: DataFrame,
      model: SupervisedModel[M],
      features: Seq[String],
      idCol: String,
      weightCol: Option[String],
      idsToInvestigate: Seq[String]
  ): DataFrame = {

    val rowsToInvestigate = df.where(df.col(idCol) isin idsToInvestigate).collect()
    val computed = rowsToInvestigate
      .flatMap(row => computeShapleyForSample(df, idCol, model, row, features, weightCol))
    spark.createDataFrame(computed).toDF(Seq("feature", "contribution", "weight"): _*)

  }

  def computePermutations(partitionIndex: Int, features: Seq[String]): Iterator[String] = {
    RandBasis
      .withSeed(partitionIndex)
      .permutation(features.size)
      .draw()
      .map(idx => features(idx))
      .toIterator
  }

  /** Computes the shapley marginal contribution for each feature in the feature vector over all
    * samples in the partition. The algorithm is based on a monte-carlo approximation:
    * https://christophm.github.io/interpretable-ml-book/shapley.html#fn42
    *
    * @param partitionIndex
    *   : Index of spark partition which will serve as a seed to numpy
    * @param randRows
    *   : Sampled rows of the dataset in the partition
    * @param rowToInvestigate
    *   Feature vector for which we need to compute shapley scores
    * @param model
    *   Spark ml Model object which implements the predict function.
    * @param weightColName
    *   column name with row weights to use when sampling the training set
    * @return
    *   Iterator of tuple of feature and shapley marginal contribution
    */
  def computeShapleyFeature(
      partitionIndex: Int,
      idCol: String,
      randRows: Iterator[Row],
      rowToInvestigate: Row,
      features: Seq[String],
      featurePermutations: IndexedSeq[String],
      weightColName: Option[String] = None
  ): Iterator[ShapleyFeature] = {

    /* We cycle through permutations in cases where the number of samples is more than
     the number of features */

    val permutationIter = Iterator.continually(featurePermutations.permutations).flatten

    val featureVectorRows = collection.mutable.Buffer.empty[ShapleyFeature]

    randRows.foreach { randRow =>
      /* take sample z from training set */
      val weight             = if (weightColName.isDefined) randRow.getAs(weightColName.get) else 1d
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

  def computeShapleyScore[M <: Model[M]](
      featureVectorRows: DataFrame,
      model: SupervisedModel[M],
      featurePermutations: IndexedSeq[String],
      featureNames: Seq[String]
  ): Map[String, (Double, Double)] = {

    val explodedFeatures = featureVectorRows
      .withColumn("features", expr("explode(features)"))
      .withColumn("feature", expr("features._1"))
      .withColumn("value", expr("features._2"))
      .groupBy("id")
      .pivot("feature")
      .agg(first("value"))
      .join(featureVectorRows, usingColumn = "id")
      .drop("features")

    val vectorAssembler = new VectorAssembler()
      .setInputCols(featureNames.toArray)
      .setOutputCol("features")

    val assembledFeatures = vectorAssembler.transform(explodedFeatures)

    val predictions  = model.predictionsWithProba(assembledFeatures)
    val predictProba = predictions.drop("prediction", "features").collect()

    val weights = predictProba.map(_.getAs[Double]("weight")).toSet.toSeq

    val featureIterator =
      Iterator.continually(featurePermutations.permutations).flatten.flatten

    val scores = for {
      idx <- predictProba.indices by 2
      feature = featureIterator.next()
    } yield {
      val marginalContribution = predictProba(idx).getAs[Double]("probability") - predictProba(
        idx + 1
      ).getAs[Double]("probability")
      /* There is one weight added per random row visited.
         For each random row visit, we generate 2 predictions for each required feature.
         Therefore, to get index into rand_row_weights, we need to divide
         prediction index by 2 * number of features, and take the floor of this
       */
      val weight = weights(idx / featureNames.size * 2)
      ShapleyScore(feature, marginalContribution, weight)
    }

    scores.groupBy(_.feature).mapValues { scs =>
      val (contributions, weights) = scs.map(sc => (sc.contribution, sc.weight)).unzip
      val bWeights                 = DenseVector.apply(weights.toArray)
      val bContributions           = DenseVector.apply(contributions.toArray)

      (breeze.linalg.sum(bWeights * bContributions), breeze.linalg.sum(bWeights))

    }

  }

  /** Compute shapley values for all features in a given feature vector of interest
    *
    * @param df
    *   : Training dataset
    *
    * @param model
    *   a Model object which implements the predictProba function.
    * @param rowToInvestigate
    *   : Feature vector for which we need to compute shapley scores
    * @param weightColName
    *   : column name with row weights to use when sampling the training set
    * @return
    *   Dictionary of feature mapping to its corresponding shapley value.
    */

  def computeShapleyForSample[M <: Model[M]](
      df: DataFrame,
      idCol: String,
      model: SupervisedModel[M],
      rowToInvestigate: Row,
      features: Seq[String],
      weightColName: Option[String] = None
  ): Map[String, (Double, Double)] = {

    import spark.implicits._

    val randomFeaturePermutation = df.rdd
      .mapPartitionsWithIndex { (idx, _) =>
        computePermutations(idx, features)
      }
      .collect()

    val featureVectorRows = df.rdd
      .mapPartitionsWithIndex(
        { (idx, rows) =>
          computeShapleyFeature(
            idx,
            idCol,
            rows,
            rowToInvestigate,
            features,
            randomFeaturePermutation,
            weightColName
          )
        },
        preservesPartitioning = true
      )
      .toDF()

    computeShapleyScore(featureVectorRows, model, randomFeaturePermutation, features)
  }
}
