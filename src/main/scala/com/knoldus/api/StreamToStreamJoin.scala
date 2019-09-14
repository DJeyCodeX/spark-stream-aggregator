package com.knoldus.api

import java.sql.Timestamp

import com.knoldus.model.{GpsDetails, ImageDetails}
import org.apache.avro.generic.GenericData
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class StreamToStreamJoin(spark: SparkSession) {

  def aggregateOnWindow(imageStream: Dataset[ImageDetails], gpsDetails: Dataset[GpsDetails], win: Long) = {
    spark.udf.register("time_in_milliseconds", (str: String) => Timestamp.valueOf(str).getTime)
      imageStream.withWatermark("timestamp", "1 seconds").join(
      gpsDetails.withWatermark("gpsTimestamp", "1 seconds"),
      expr(
        s"""
            cameraId = gpscameraId AND
            abs(time_in_milliseconds(timestamp) - time_in_milliseconds(gpsTimestamp)) <= $win
            """.stripMargin)
    ).selectExpr("ImageId", "timestamp", "gpsTimestamp", "abs(time_in_milliseconds(gpsTimestamp) - time_in_milliseconds(timestamp)) as diff")
      /*.withWatermark("timestamp", "1 seconds")*/
      .groupBy("ImageId", "timestamp")
      .agg(min("diff")).withColumnRenamed("min(diff)", "nearest")

  }

  def aggregatedWindow(imageStream: Dataset[ImageDetails], duration: String): DataFrame = {
    imageStream
      .withWatermark("timestamp", "1 seconds")
      .groupBy(window(col("timestamp"), duration, duration ), col("ImageId")).agg(collect_list("ImageId"))
      .withColumn("window", col("window").cast(StringType))
      .withColumnRenamed("collect_list(ImageId)", "images")
  }


  class WindowAggregator extends UserDefinedAggregateFunction {

    override def inputSchema: StructType = StructType(Seq(StructField("timestamp", StringType, false)))

    override def bufferSchema: StructType = StructType(Seq(
      StructField("first_log_time", StringType, false), StructField("last_log_time", StringType, false)
    ))

    override def dataType: DataType = StringType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, Long.MaxValue)
      buffer.update(1, Long.MinValue)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val sessionStartTime = buffer.getLong(0)
      val sessionLastLogTime = buffer.getLong(1)
      val logTime = input.getLong(0)
      if (logTime < sessionStartTime) {
        buffer.update(0, logTime)
      }
      if (logTime > sessionLastLogTime) {
        buffer.update(1, logTime)
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val buffer1StartTime = buffer1.getLong(0)
      val buffer2StartTime = buffer2.getLong(0)
      if (buffer2StartTime < buffer1StartTime) {
        buffer1.update(0, buffer2StartTime)
      }
      val buffer1EndTime = buffer1.getLong(1)
      val buffer2EndTime = buffer2.getLong(1)
      if (buffer2EndTime > buffer1EndTime) {
        buffer1.update(1, buffer2EndTime)
      }
    }

    override def evaluate(buffer: Row): Any = {
      val sessionStartTime = buffer.getLong(0)
      val sessionLastLogTime = buffer.getLong(1)
      sessionLastLogTime - sessionStartTime
    }
  }

}