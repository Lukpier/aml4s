package aml4s.model

import org.apache.spark.ml.Model
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, exp, expr, udf}

object Models {

  sealed trait CustomModel[M <: Model[M]] {

    def model: M

  }

  case class UnsupervisedModel[M <: Model[M]](model: M) extends CustomModel[M] {


  }

  case class SupervisedModel[M <: Model[M]](model: M, explainClass: Int = 1) extends CustomModel[M] {

    lazy val extractProbabilityClass: UserDefinedFunction = udf((probability: org.apache.spark.ml.linalg.Vector) => {
      probability(explainClass)
    })

    def predictionsWithProba(df: DataFrame): DataFrame = {
      val columns = df.columns ++ Seq("prediction", "probability")
      val result = model
        .transform(df)
        .select(columns.head, columns.tail: _*)
      result
        .withColumn("probability", extractProbabilityClass(result("probability")))
    }

  }

}
