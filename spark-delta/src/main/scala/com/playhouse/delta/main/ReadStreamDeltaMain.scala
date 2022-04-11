package com.playhouse.delta.main

import com.playhouse.delta.infra.spark.SparkSessionBuilder
import com.playhouse.delta.sensorDataLogger
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object ReadStreamDeltaMain {
  def main(args: Array[String]): Unit = {
    Try{
      implicit val spark: SparkSession = SparkSessionBuilder().build(appName = "spark-delta")
      sensorDataLogger.info("Got spark...")

      import io.delta.implicits._
      spark.readStream.delta(s"s3a://delta/travel.db/hotel").createTempView("delta_hotel")
      val df = spark.sql("select * from delta_hotel where day > 20201010")
      val query = df.writeStream
        .outputMode("append")
        .format("console")
        .start()

      query.awaitTermination()




    } match {
      case Success(_) => sensorDataLogger.info("Calling from finish job...SUCCESSSSSSS")
      case Failure(e) =>
        sensorDataLogger.error(s"OH NO... Things went wrong, Here exactly --> ${e.getMessage} ${e.getStackTrace.mkString("\n")}")
        // TODO : for debugging.
        e.printStackTrace()
        System.exit(-1)
    }
  }
}
