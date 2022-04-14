package com.playhouse.delta.main

import com.playhouse.delta.common.Constants.{databaseName, sparkCheckpointDirectory, tableName}
import com.playhouse.delta.common.TargetSystem
import com.playhouse.delta.infra.spark.SparkSessionBuilder
import com.playhouse.delta.sensorDataLogger
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object ReadStreamDeltaMain {
  def main(args: Array[String]): Unit = {
    Try{
      implicit val spark: SparkSession = SparkSessionBuilder().build(appName = "spark-delta")
      sensorDataLogger.info("Got spark...")

      spark.readStream.format("delta").table("travel.hotel").createTempView("delta_hotel")
      val df = spark.sql("select * from delta_hotel where place='Natal (RN)'")
//      val df = spark.sql(
//        s"""select sum(total) as sum_total, place, day
//           |from travel.hotel group by place, day;""".stripMargin)

      val s3databaseLocation = s"s3a://${TargetSystem.DELTA.toString}/$databaseName.db"
      val s3TableLocation = s"$s3databaseLocation/hotel_natal_rn"
      val tableCols =
        s"""travelCode int, userCode int, name string, place string, stayingDays int,
           |price float, total float, `date` date, day int""".stripMargin
//
//      spark.sql(s"GENERATE symlink_format_manifest FOR TABLE delta.`$s3TableLocation`")
//      spark.sql(
//        s"""CREATE DATABASE IF NOT EXISTS $databaseName
//           |LOCATION 's3a://${TargetSystem.DELTA.toString}/$databaseName.db'""".stripMargin)
//      spark.sql(
//        s"""CREATE EXTERNAL TABLE IF NOT EXISTS $databaseName.hotel_natal_rn($tableCols)
//            USING DELTA
//            PARTITIONED BY(day)
//            LOCATION '$s3TableLocation'""".stripMargin)
//      spark.sql(s"ALTER TABLE delta.`$s3TableLocation` SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)")

      val query = df.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("day")
        .option("checkpointLocation", sparkCheckpointDirectory)
        .toTable(s"$databaseName.hotel_natal_rn")
        //.start()

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
