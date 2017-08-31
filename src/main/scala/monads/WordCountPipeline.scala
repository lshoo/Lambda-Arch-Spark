package monads

import cats.data.Kleisli
import org.apache.spark.{SparkConf, SparkContext}
import cats.data.Kleisli._

/**
  * 
  */
trait WordCountPipeline {

  // initial SparkOperation created using companion object
  def linesOp = SparkOperation { ctx =>
    ctx.parallelize(Seq("james wades Bosh kobi ducan durant haden westbrook james wades Bosh kobi ducan james wades Bosh kobi ducan"))
  }

  def wordsOp = for {
    lines <- linesOp
  } yield {
    lines.flatMap { line => line.split("\\W+")}
      .map(_.toLowerCase)
      .filter(!_.isEmpty)
  }

  def countOp = for {
    words <- wordsOp
  } yield words.map { (_, 1) }.reduceByKey(_ + _)

  def topWordsOp(n: Int) =
    countOp.map(_.takeOrdered(n)(Ordering.by(-_._2)))

}

object WordCountPipeline extends WordCountPipeline

object WordCountPipelineApp {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf().setMaster("local[2]").setAppName("WordCountPipeline")
    )

    import WordCountPipeline._
    val topWordsMap = topWordsOp(100).run(sc)

    println(topWordsMap.toSeq)

    sc.stop

  }
}