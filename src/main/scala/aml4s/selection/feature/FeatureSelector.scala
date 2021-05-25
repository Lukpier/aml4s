package aml4s.selection.feature

import org.apache.spark.sql.DataFrame

trait FeatureSelector {

  def select(df: DataFrame): DataFrame

}
