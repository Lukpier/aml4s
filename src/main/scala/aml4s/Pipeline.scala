package aml4s

import aml4s.contribution.FeatureContribution
import aml4s.extraction.timeseries.FeatureExtractor
import aml4s.monitoring.Monitoring
import aml4s.selection.feature.FeatureSelector
import aml4s.selection.model.ModelSelector
import aml4s.source.Source
import org.apache.spark.ml.Model
import org.apache.spark.sql.DataFrame

trait Pipeline {

  def source: Source
  def models: Seq[Model[_]]

  def extractor: FeatureExtractor
  def featureSelector: FeatureSelector
  def modelSelector: ModelSelector
  def contribution: FeatureContribution
  def monitoring: Monitoring

  def run(): Unit = {

    val data: DataFrame            = source.load
    val features: DataFrame        = extractor.extractFeatures(data)
    val featureSelected: DataFrame = featureSelector.select(features)
    val model: Model[_]            = modelSelector.select(featureSelected, models)

  }

}
