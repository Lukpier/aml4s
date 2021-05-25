package aml4s.selection.model

import org.apache.spark.ml.Model
import org.apache.spark.sql.DataFrame

trait ModelSelector {

  def select(df: DataFrame, models: Seq[Model[_]]): Model[_]

}
