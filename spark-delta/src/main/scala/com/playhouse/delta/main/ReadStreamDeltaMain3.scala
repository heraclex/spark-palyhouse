package com.playhouse.delta.main

import com.playhouse.delta.common.Constants.{databaseName, sparkCheckpointDirectory}
import com.playhouse.delta.infra.spark.SparkSessionBuilder
import com.playhouse.delta.services.{PostgresInstance, PostgresSink, TotalRevenuePerHotel}
import com.playhouse.delta.sparkAppLogger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.Trigger

import java.sql.Date
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

object ReadStreamDeltaMain3 {
  def main(args: Array[String]): Unit = {
    Try{
      implicit val spark: SparkSession = SparkSessionBuilder().build(appName = "spark-read-delta3")
      sparkAppLogger.info("Got spark...")

      spark.readStream.format("delta").table(s"$databaseName.hotel2").createTempView("tmp")
      val df = spark.sql("select name as hotel_name, date, total from tmp")

      val now = java.time.Instant.now
      val batchInterval = 2000
//      df.writeStream.trigger(Trigger.ProcessingTime(batchInterval))
//        .foreachBatch({ (batchDF: DataFrame, batchId: Long) =>
//          println(now.plusMillis(batchId * batchInterval.milliseconds))
//        })
//        .outputMode(...)
//      .start()

      import spark.implicits._
      val query = df.writeStream
        .trigger(Trigger.ProcessingTime(batchInterval))
        .foreachBatch({ (batchDF: DataFrame, batchId: Long) =>
          System.out.println(now.plusMillis(batchId * batchInterval.milliseconds.toMillis))

          val updateTime = batchId * batchInterval.milliseconds.toMillis
          val totalRevenuePerHotel = batchDF.map {
            case Row(hotel_name: String, date: Date, day: Long, total: Float) =>
              TotalRevenuePerHotel(updateTime: Long, hotel_name, date, total)
          } collect()

          PostgresSink(PostgresInstance(
            host="localhost", port=5432,
            userName="postgres", password="postgres", db="postgres"
          )).process(totalRevenuePerHotel)
        })
        .outputMode("complete").start()

      query.awaitTermination()

    } match {
      case Success(_) => sparkAppLogger.info("Calling from finish job...SUCCESSSSSSS")
      case Failure(e) =>
        sparkAppLogger.error(s"OH NO... Things went wrong, Here exactly --> ${e.getMessage} ${e.getStackTrace.mkString("\n")}")
        // TODO : for debugging.
        e.printStackTrace()
        System.exit(-1)
    }
  }
}
