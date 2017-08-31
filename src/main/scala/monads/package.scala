import cats.data.Kleisli
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * TODO
  */
package object monads {

  type SparkOperationT[A] = Kleisli[RDD, SparkContext, A]
}
