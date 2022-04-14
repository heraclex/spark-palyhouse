package com.playhouse.delta.main

import com.playhouse.delta.infra.spark.SparkSessionBuilder
import com.playhouse.delta.sensorDataLogger
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object WriteHiveDeltaMain {
  def main(args: Array[String]): Unit = {
    Try{
      implicit val spark: SparkSession = SparkSessionBuilder().build(appName = "spark-delta")
      sensorDataLogger.info("Got spark...")

      val tableCols =
        s"""travelCode int, userCode int, name string, place string, stayingDays int,
           |price float, total float, `date` date, day int""".stripMargin

      val database = "travel"
      val tableName = "hotel"
      val s3DatabaseLocation = s"s3a://delta/$database.db"
      val s3TableLocation = s"$s3DatabaseLocation/$tableName/"

      // https://github.com/delta-io/connectors/issues/71
      spark.read.option("delimiter", ",")
        .option("header", "true").csv("./src/main/resources/hotels.csv").createTempView("tmp")

      val df = spark.sql(
        s"""
           |select cast(travelCode as int) as travelCode,
           |cast(userCode as int) as userCode,
           |name,place,
           |cast(days as int) as stayingDays,
           |cast(price as float) as price,
           |cast(total as float) as total,
           |to_date(date, 'MM/dd/yyyy') as date, cast(date_format(to_date(date, 'MM/dd/yyyy'), 'yyyyMMdd') as int) as day
           |from tmp
           |""".stripMargin)

      df.show(10,true)
      df.write.format("delta").partitionBy("day").mode("overwrite").save(s3TableLocation)

      // support other processing engine
      spark.sql(s"GENERATE symlink_format_manifest FOR TABLE delta.`$s3TableLocation`")

      // support Hive
      // https://docs.delta.io/latest/presto-integration.html#step-1-generate-manifests-of-a-delta-table-using-apache-spark
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $database LOCATION '$s3DatabaseLocation'")
//      spark.sql(s"DROP TABLE IF EXISTS $database.$tableName")
      spark.sql(s""" CREATE TABLE IF NOT EXISTS $database.$tableName($tableCols)
            USING DELTA
            PARTITIONED BY(day)
            LOCATION '$s3TableLocation'""".stripMargin)



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
