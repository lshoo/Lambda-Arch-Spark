package com.knoldus.speed

import java.sql.Timestamp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import com.knoldus.udfs._
import org.apache.spark.sql.functions._

/**
  * Created by narayan on 12/11/16.
  */
object ViewHandler {


  def createAllView(sparkContext: SparkContext, tweets: DStream[String]) = {
    createViewForFriendCount(sparkContext, tweets)
  }

  def createViewForFriendCount(sparkContext: SparkContext, tweets: DStream[String]) = {

    tweets.foreachRDD { (rdd: RDD[String], time: Time) =>
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      val tweets: DataFrame = spark.sqlContext.read.json(rdd)
      tweets.createOrReplaceTempView("tweets")

      import spark.implicits._
      spark.udf.register("gt500", gt500 _)
      spark.udf.register("afterDay", afterDay _)
      val nowTime = new Timestamp(System.currentTimeMillis())

      //val wordCountsDataFrame: DataFrame = spark.sql("SELECT userId,createdAt, friendsCount from tweets Where friendsCount > 500 ")
      val wordCountsDataFrame: DataFrame = spark.sql(s"SELECT userId,createdAt, friendsCount from tweets Where gt500(friendsCount) and afterDay(createAt, $nowTime)")
      val res: DataFrame = wordCountsDataFrame.withColumnRenamed("userId","userid").withColumnRenamed("createdAt","createdat")
        .withColumnRenamed("friendsCount","friendscount")
        .filter(after($"createdat", lit(nowTime)))
      res.write.mode(SaveMode.Append)
        .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "friendcountview", "keyspace" -> "realtime_view"))
        .save()
      wordCountsDataFrame.show(false)
      wordCountsDataFrame.printSchema()

    }
  }
}
