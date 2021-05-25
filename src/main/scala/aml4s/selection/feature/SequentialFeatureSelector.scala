package aml4s.selection.feature
import org.apache.spark.sql.DataFrame

class SequentialFeatureSelector extends FeatureSelector {

  override def select(df: DataFrame): DataFrame = ???

}
