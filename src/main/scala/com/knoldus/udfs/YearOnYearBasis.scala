package com.knoldus.udfs

import java.sql.{Date, Timestamp}
import java.time.ZoneId
import java.time.temporal.ChronoField

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

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

  override def dataType = DataTypes.DoubleType

  override def deterministic = false

  override def initialize(buffer: MutableAggregationBuffer) = {
    buffer.update(0, 0.0)
    buffer.update(1, 0.0)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row) = {
    if (current.in(input.getAs[Date](1))) {
      buffer(0) = buffer.getAs[Double](0) + input.getAs[Double](0)
    }

    val previous = DateRange(subtractOneYear(current.startDate), subtractOneYear(current.endDate))
    if (previous.in(input.getAs[Date](1))) {
      buffer(1) = buffer.getAs[Double](0) +  input.getAs[Double](0)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
  }

  override def evaluate(buffer: Row) = {
    if (buffer.getDouble(1) == 0.0) 0.0
    else
      (buffer.getDouble(0) - buffer.getDouble(1)) / buffer.getDouble(1) * 100
  }

  private def subtractOneYear(date: Timestamp) = {
    new Timestamp(date.toLocalDateTime.minusYears(1).atZone(ZoneId.systemDefault).toInstant.toEpochMilli)
    //new Timestamp(date.toLocalDateTime.get(ChronoField.MILLI_OF_SECOND))
  }
}
