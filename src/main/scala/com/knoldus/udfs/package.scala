package com.knoldus

import java.sql.Timestamp

import org.apache.spark.sql.functions._

/**
  * http://zhangyi.farbox.com/post/framework/udf-and-udaf-in-spark
  * 自定义的udf
  */
package object udfs {

  def len(bookTitle: String): Int = bookTitle.length

  val gt500: Int =>  Boolean = _ > 500

  def afterDay(actual: Timestamp, toCompare: Timestamp): Boolean = actual.after(toCompare)

  val after  = udf((t1: Timestamp, t2: Timestamp) => t1.after(t2))
}
