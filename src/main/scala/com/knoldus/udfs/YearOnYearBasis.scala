package com.knoldus.udfs

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}

/**
  * 自定义　udf
  */
case class DateRange(startDate: Timestamp, endDate: Timestamp) {
  def in(targetDate: Date): Boolean = {
    targetDate.before(endDate) && targetDate.after(startDate)
  }
}

class YearOnYearBasis(current: DateRange) extends UserDefinedAggregateFunction {

  override def inputSchema = {
    StructType(StructField("metric", DoubleType) :: StructField("timeCategory", DateType) :: Nil)
  }

  override def bufferSchema = {
    StructType(StructField("sumOfCurrent", DoubleType) :: StructField("sumOfPrevious", DoubleType) :: Nil)
  }

  override def dataType = ???

  override def deterministic = ???

  override def initialize(buffer: MutableAggregationBuffer) = ???

  override def update(buffer: MutableAggregationBuffer, input: Row) = ???

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = ???

  override def evaluate(buffer: Row) = ???
}
