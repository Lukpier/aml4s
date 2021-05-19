package aml4s.timeseries

import breeze.linalg.DenseVector
import aml4s.helper.SparkHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, sort_array, struct, udf}

object Timeseries {

  import TimeserieFeatureCalculators._

  lazy val udfs = Map(
    ("zero_crossing", udf((ts: Seq[Double]) => zeroCrossing(DenseVector.apply(ts.toArray)))),
    ("fft", udf((ts: Seq[Double]) => fftCoefficient(DenseVector.apply(ts.toArray)))),
    ("minimum", udf((ts: Seq[Double]) => minimum(DenseVector.apply(ts.toArray)))),
    ("maximum", udf((ts: Seq[Double]) => maximum(DenseVector.apply(ts.toArray)))),
    ("kurtosis", udf((ts: Seq[Double]) => kurtosis(ts.toArray))),
    ("skewness", udf((ts: Seq[Double]) => skewness(ts.toArray))),
    ("mean", udf((ts: Seq[Double]) => mean(DenseVector.apply(ts.toArray)))),
    ("variance", udf((ts: Seq[Double]) => variance(DenseVector.apply(ts.toArray)))),
    (
      "symmetry",
      udf((ts: Seq[Double]) =>
        symmetryLooking(DenseVector.apply(ts.toArray), (1 to 20).map(_ + 0.05))
      )
    )
  )

  implicit class TimeserieDF(df: DataFrame) {

    def wideFormat(idCols: Seq[String], timeCol: String, featureCols: Seq[String]): DataFrame = {

      val collectCols = featureCols.map { selectedCol =>
        sort_array(collect_list(struct(timeCol, selectedCol))).alias(selectedCol)
      }

      val grouped =
        df.groupBy(idCols.head, idCols.tail: _*).agg(collectCols.head, collectCols.tail: _*)

      featureCols.foldLeft(grouped) { (tempDF, column) =>
        tempDF.withColumn(column, col(s"$column.$column"))
      }

    }

    def extractFeatures(inputCols: Seq[String]): DataFrame = {

      inputCols.foldLeft(df) { (tempDf, inputCol) =>
        {

          SparkHelper.applyForArray(
            df,
            inputCol, {

              udfs.foldLeft(tempDf) { case (extractDf, (fnName, fn)) =>
                extractDf.withColumn(s"${inputCol}_$fnName", fn(df.col(inputCol)))
              }

            }
          ).drop(col(inputCol)) /* dropping original feature */

        }

      }
    }
  }

}
