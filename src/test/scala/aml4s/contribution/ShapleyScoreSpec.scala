package aml4s.contribution

import aml4s.model.Models.SupervisedModel
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ShapleyScoreSpec extends AnyWordSpec with Matchers with DataFrameSuiteBase {

  "a bla" should {
    "bla" in {

      import spark.implicits._

      val df = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("sep", ",")
        .csv("src/test/resources/data/sample_logistic_regression.csv")

      val shapleyScore = ShapleyScoreCalculator()(spark)
      val assembler    = new VectorAssembler()
      val cols         = df.columns.map(col => col.replace(" ", "_")).map(_.replace(".", ""))
      val featureCols  = Set("SOP", "GRE_Score", "CGPA")
      val renamed      = df.toDF(cols.toArray: _*)
      assembler.setInputCols(featureCols.toArray)
      assembler.setOutputCol("features")
      val features = assembler.transform(renamed).select("Serial_No", "features")
      val df2 = renamed
        .join(
          features,
          usingColumn = "Serial_No"
        )
        .select("Serial_No", "features", "Research")
      df2.show()
      df2.printSchema()
      val model = SupervisedModel(
        new LogisticRegression()
          .setMaxIter(100)
          .setRegParam(0.02)
          .setElasticNetParam(0.8)
          .setLabelCol("Research")
          .setFeaturesCol("features")
          .fit(df2)
      )
      shapleyScore.computeShapleyForSample(
        renamed,
        "Serial_No",
        model,
        renamed.rdd.first(),
        Seq("SOP", "LOR", "CGPA")
      )

    }
  }

}
