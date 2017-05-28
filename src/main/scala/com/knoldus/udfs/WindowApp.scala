package com.knoldus.udfs

import org.apache.spark._
import org.apache.spark.sql._

/**
  * https://community.cloud.databricks.com/?o=3725561310451767#notebook/3706476727933423/command/1232554033955866
  * https://my.oschina.net/corleone/blog/755393
  */
object WindowApp {

  case class Score(name: String, lesson: String, score: Int)

  def main(args: Array[String]): Unit = {


    val scores = Seq(
      Score("A", "Math", 100), Score("B", "Math", 100), Score("C", "Math", 99), Score("D", "Math", 48), Score("E", "Math", 50),
      Score("A", "E", 100), Score("B", "E", 99), Score("C", "E", 99), Score("D", "E", 98)
    )

    val conf = new SparkConf().setAppName("WindowUDF").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    val scoresDF = scores.toDF
    scoresDF.createOrReplaceTempView("scores")

    // using sql
    val sqlStats = "select".concat(" name, lesson, score, ntile(2) over (partition by lesson order by score desc) as ntile_2").
      concat(", ntile(3) over (partition by lesson order by score desc) as ntile_3").
      concat(", row_number() over (partition by lesson order by score desc) as row_number").
      concat(", rank() over (partition by lesson order by score desc) as rank").
      concat(", dense_rank() over (partition by lesson order by score desc) as dense_rank").
      concat(", percent_rank() over (partition by lesson order by score desc) as percnet_rank").  // (rank -1) / (count(score) -1)
      concat(" from scores order by lesson, name, score ")
    spark.sql(sqlStats).show

    // useing DataFrame
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._
    /*
    val window_spec = Window.partitionBy("lesson").orderBy(scoresDF("score").desc)  // 窗口函数中公用的子句
    val dfStats = scoresDF.select($"name", $"lesson", $"score",
      ntile(2).over(window_spec).as("ntile_2"),
      ntile(3).over(window_spec).as("ntile_3"),
      row_number().over(window_spec).as("row_number"),
      rank() over window_spec as("rank"),
      dense_rank() over window_spec as("dense_rank"),
      percent_rank() over window_spec as "percent_rank"
    ).orderBy($"lesson", $"score".desc, $"name")
    dfStats.show
    */
    val window_clause = Window.partitionBy("lesson").orderBy($"score".desc)
    val window_spec2 = window_clause.rangeBetween(0, 101)
    val window_spec3 = window_clause.rowsBetween(-1, 0)  // 相对范围， -1表示当前行的前一行

    val stats2 = scoresDF.select($"name", $"lesson", $"score",
      ($"score" - first("score").over(window_spec3)).as("score-last_score"),
      (min($"score").over(window_spec2)).as("min_score"),
      //(min(scoresDF("score")).over(window_spec2)).as("min_score"),
      ($"score" - min($"score").over(window_spec2)).as("score-min"),
      (max($"score").over(window_spec2)).as("max_score"),
      ($"score" - max($"score").over(window_spec2)).as("score-max"),
      (avg($"score").over(window_spec2)).as("avg_score"),
      ($"score" - avg($"score").over(window_spec2)).as("score-avg")
    ).orderBy($"lesson", $"score".desc)
    stats2.show
  }
}
