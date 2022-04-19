package com.playhouse.delta.main

import com.playhouse.delta.common._
import com.playhouse.delta.common.Constants._
import com.playhouse.delta.infra.spark.SparkSessionBuilder
import com.playhouse.delta.sparkAppLogger
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object WriteToDeltaMain {
  def main(args: Array[String]): Unit = {
    Try{
      implicit val spark: SparkSession = SparkSessionBuilder().build(appName = "spark-delta")
      sparkAppLogger.info("Got spark...")

      val s3databaseLocation = s"s3a://${TargetSystem.DELTA.toString}/$databaseName.db"
      val s3TableLocation = s"$s3databaseLocation/$tableName"
      val tableCols =
        s"""travelCode int, userCode int, name string, place string, stayingDays int,
           |price float, total float, `date` date, day int""".stripMargin
      val query = s"""
                     |select cast(travelCode as int) as travelCode,
                     |cast(userCode as int) as userCode,
                     |name,place,
                     |cast(days as int) as stayingDays,
                     |cast(price as float) as price,
                     |cast(total as float) as total,
                     |to_date(date, 'MM/dd/yyyy') as date,
                     |cast(date_format(to_date(date, 'MM/dd/yyyy'), 'yyyyMMdd') as int) as day
                     |from tmp""".stripMargin

      // https://github.com/delta-io/connectors/issues/71
      spark.read.option("delimiter", ",")
        .option("header", "true").csv("./src/main/resources/hotels_part1.csv")
        .createTempView("tmp")

      val df = spark.sql(query)

      df.show(10,true)

      spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName LOCATION '$s3databaseLocation'".stripMargin)
      df.write.format("delta").partitionBy("day")
        .mode("append").saveAsTable(s"$databaseName.$tableName")

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
