package monads

import cats.Monad
import org.apache.spark.SparkContext

/**
  * http://www.stephenzoio.com/creating-composable-data-pipelines-spark/
  */
case class SparkOperation[+A](run: SparkContext => A) {

  // execute the transformations
  //def run(ctx: SparkContext): A

  def map[B](f: A => B): SparkOperation[B] = SparkOperation { ctx => f(this.run(ctx))}

  def flatMap[B](f: A => SparkOperation[B]): SparkOperation[B] = SparkOperation { ctx => f(this.run(ctx)).run(ctx)}

}

object SparkOperation {
  //def apply[A](f: SparkContext => A) = SparkOperation(f)

  implicit val monad = new Monad[SparkOperation] {
    override def pure[A](a: A): SparkOperation[A] = SparkOperation(ctx => a)

    override def flatMap[A, B](fa: SparkOperation[A])(f: (A) => SparkOperation[B]): SparkOperation[B] = {
      fa.flatMap(f)
    }

    override def tailRecM[A, B](a: A)(f: (A) => SparkOperation[Either[A, B]]): SparkOperation[B] = SparkOperation( ctx =>
      f(a).run(ctx) match {
      case  Left(_) => throw new Exception(s"init SparkOperation use $a error")
              
      case Right(r) => r
    } )
  }

}

