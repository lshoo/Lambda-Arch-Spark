import cats.data.Kleisli
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

/**
  * TODO
  */
package object monads {

  type SparkOperationT[A] = Kleisli[RDD, SparkContext, A]

  type SparkSessionOp[A, B] = Kleisli[Dataset, A, B]
}
