package aml4s.contribution

import org.apache.spark.ml.Model
import org.apache.spark.sql.DataFrame

trait FeatureContribution {

  def analyze[M <: Model[M]](df: DataFrame, model: M): DataFrame

}
