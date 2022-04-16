package com.playhouse.delta.main

import com.playhouse.delta.common.Constants.{databaseName, sparkCheckpointDirectory}
import com.playhouse.delta.common.TargetSystem
import com.playhouse.delta.infra.spark.SparkSessionBuilder
import com.playhouse.delta.sparkAppLogger
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object ReadStreamDeltaMain2 {
  def main(args: Array[String]): Unit = {
    Try{
      implicit val spark: SparkSession = SparkSessionBuilder().build(appName = "spark-read-delta2")
      sparkAppLogger.info("Got spark...")

      spark.readStream.format("delta").table(s"$databaseName.hotel1").createTempView("tmp")
      val df = spark.sql(
        s"""select name as hotel_name, date, day, sum(total) as total
           |from tmp group by name, date, day""".stripMargin)

      val query = df.writeStream.format("delta")
        .outputMode("complete")
        .partitionBy("day")
        .option("checkpointLocation", s"$sparkCheckpointDirectory/$databaseName/hotel2")
        .toTable(s"$databaseName.hotel2")
        //.start()

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
