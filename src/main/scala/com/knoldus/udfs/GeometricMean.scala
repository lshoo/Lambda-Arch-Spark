package com.knoldus.udfs

import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._

/**
  * https://databricks.com/blog/2015/09/16/apache-spark-1-5-dataframe-api-highlights.html
  */
class GeometricMean extends UserDefinedAggregateFunction with Serializable {
  def inputSchema: StructType = StructType(StructField("value", DoubleType) :: Nil)

  def bufferSchema: StructType = StructType(
    StructField("count", LongType) :: StructField("product", DoubleType) :: Nil
  )

  def dataType: DataType = DoubleType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1.0
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
  }

  def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
  }

}
