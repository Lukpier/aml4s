package aml4s.helper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.ArrayType

object SparkHelper {

  // TODO: make it working with generics
  def applyForArray(df: DataFrame, inputCol: String, fn: => DataFrame): DataFrame = {

    df.schema(inputCol).dataType match {
      case _: ArrayType => fn
      case _ => // can't compute fft
        df
    }
  }

}
