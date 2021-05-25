package aml4s.model

import org.apache.spark.ml.Model
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}

object Models {

 sealed trait CustomModel[M <: Model[M]] {

   def model: M

 }

 case class UnsupervisedModel[M <: Model[M]](model: M) extends CustomModel[M] {



 }

  case class SupervisedModel[M <: Model[M]](model: M) extends CustomModel[M] {

    def predictionsWithProba(df: DataFrame): DataFrame = {
      val columns = df.columns ++ Seq("prediction", "probability")
      model
        .transform(df)
        .select(columns.head, columns.tail: _*)
    }

  }

}
