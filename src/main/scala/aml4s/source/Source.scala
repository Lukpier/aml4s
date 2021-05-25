package aml4s.source

import org.apache.spark.sql.DataFrame

trait Source {

  def load: DataFrame

}
