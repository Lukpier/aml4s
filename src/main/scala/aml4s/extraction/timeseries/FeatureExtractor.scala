package aml4s.extraction.timeseries

import org.apache.spark.sql.DataFrame

trait FeatureExtractor {

  def extractFeatures(df: DataFrame): DataFrame

}
