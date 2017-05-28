package com.knoldus.udfs

import java.sql.Timestamp

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Udf example
  */
object UdfApp {
  def main(args: Array[String]): Unit = {

    //val spark = SparkSession.builder.config(sparkContext.getConf).getOrCreate()
    val conf = new SparkConf().setAppName("TestUDF").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val sales = Seq(
      (1, "Widget Co", 1000.00, 0.00, "AZ", "2014-01-01"),
      (2, "Acme Widget", 2000.00, 500.00, "CA", "2014-02-01"),
      (3, "Widgetry", 1000.00, 200.00, "CA", "2015-01-11"),
      (4, "Widgets R Us", 2000.00, 0.0, "CA", "2015-02-19"),
      (5, "Ye Olde Widgete", 3000.00, 0.0, "MA", "2015-02-28")
    )

    val salesRows = spark.sparkContext.parallelize(sales, 4)
    val salesDF = salesRows.toDF("id", "name", "sales", "discount", "state", "saleDate")
    salesDF.createTempView("sales")

    val current = DateRange(Timestamp.valueOf("2015-01-01 00:00:00"), Timestamp.valueOf("2015-12-31 00:00:00"))
    val yearOnYear = new YearOnYearBasis(current)

    spark.udf.register("yearOnYear", yearOnYear)

    val dataFrame = spark.sql("select yearOnYear(sales, saleDate) as yearOnYear from sales")
    dataFrame.show()
  }
}
