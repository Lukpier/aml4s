package aml4s

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._
import matchers._
import org.scalatest.wordspec.AnyWordSpec

class TimeseriesSpec extends AnyWordSpec with should.Matchers with DataFrameSuiteBase {

  "A timeserie df" when {

    "wideFormat method is called" should {

      "group by id cols and collect features into arrays" in {
        import aml4s.timeseries.Timeseries.TimeserieDF

        val df = spark.read
          .option("inferSchema", "true")
          .option("header", "true")
          .csv("src/test/resources/data/sample.csv")

        val count   = df.groupBy(df.col("id")).count().count()
        val columns = df.columns.toSet

        val wide = df.wideFormat(
          idCols = Seq("id"),
          timeCol = "time",
          featureCols = Seq("feature1", "feature2", "feature3")
        )
        wide.show()
        wide.count() shouldBe count
        wide.columns should contain theSameElementsAs (columns - "time")

      }

    }

    "extractFeatures method is called" should {

      "apply feature calculators to selected features" in {
        conf.set("spark.debug.maxToStringFields", "100")

        import aml4s.timeseries.Timeseries.TimeserieDF

        val df = spark.read
          .option("inferSchema", "true")
          .option("header", "true")
          .csv("src/test/resources/data/sample.csv")

        val wide = df.wideFormat(
          idCols = Seq("id"),
          timeCol = "time",
          featureCols = Seq("feature1", "feature2", "feature3")
        )
        val features = wide.extractFeatures(Seq("feature1", "feature2", "feature3"))

        features.show(5)

      }

    }

  }

}
