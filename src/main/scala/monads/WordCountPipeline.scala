package monads

import cats.data.Kleisli
import org.apache.spark.{SparkConf, SparkContext}
import cats.data.Kleisli._
import org.apache.spark.sql.{Dataset, SparkSession}

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

  /*val linesDS: SparkSessionOp[SparkSession, String] = Kleisli { spark =>
    spark.createDataset(Seq("james wades Bosh kobi ducan durant haden westbrook james wades Bosh kobi ducan james wades Bosh kobi ducan"))
  }

  val wordsDS: SparkSessionOp[String, Array[String]] = Kleisli { lines =>
    lines.split("\\W+").map(_.toLowerCase).filter(!_.isEmpty)
  }

  val countDS: SparkSessionOp[Dataset[Array[String]], Array[(String, Int)]] = Kleisli { ds =>
    for {
      words <- ds
    } yield words.map { (_, 1)}
  }

  val reduceDS: SparkSessionOp[Dataset[Array[(String, Int)]], Map[String, Int]] = Kleisli { ds =>
    ds.map { as =>
      as.foldRight(Map.empty[String, Int]) { (op, acc) =>
        acc.get(op._1) match {
          case Some(cur) =>
            acc.updated(op._1, op._2 + cur)
          case None => acc + op
        }
      }
    }
  }

  def wordcount = for {
    l <- linesDS
    w <- wordsDS(l)
    c <- countDS(w)
    r <- reduceDS(c)
  } yield r*/
}

object WordCountPipeline extends WordCountPipeline

object WordCountPipelineApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCountPipeline")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext


    import WordCountPipeline._
    val topWordsMap = topWordsOp(100).run(sc)

    println(topWordsMap.toSeq)
    
    sc.stop

  }
}