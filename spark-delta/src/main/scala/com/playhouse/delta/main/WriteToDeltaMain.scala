package com.playhouse.delta.main

import com.playhouse.delta.common._
import com.playhouse.delta.common.Constants._
import com.playhouse.delta.infra.spark.SparkSessionBuilder
import com.playhouse.delta.sensorDataLogger
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object WriteToDeltaMain {
  def main(args: Array[String]): Unit = {
    Try{
      implicit val spark: SparkSession = SparkSessionBuilder().build(appName = "spark-delta")
      sensorDataLogger.info("Got spark...")

      val s3databaseLocation = s"s3a://${TargetSystem.DELTA.toString}/$databaseName.db"
      val s3TableLocation = s"$s3databaseLocation/$tableName"
      val tableCols =
        s"""travelCode int, userCode int, name string, place string, stayingDays int,
           |price float, total float, `date` date, day int""".stripMargin

      // https://github.com/delta-io/connectors/issues/71
      spark.read.option("delimiter", ",")
        .option("header", "true").csv("./src/main/resources/hotels_part3.csv")
        .createTempView("tmp")

      val df = spark.sql(
        s"""
           |select cast(travelCode as int) as travelCode,
           |cast(userCode as int) as userCode,
           |name,place,
           |cast(days as int) as stayingDays,
           |cast(price as float) as price,
           |cast(total as float) as total,
           |to_date(date, 'MM/dd/yyyy') as date,
           |cast(date_format(to_date(date, 'MM/dd/yyyy'), 'yyyyMMdd') as int) as day
           |from tmp
           |""".stripMargin)

      df.show(10,true)
      df.write.format("delta").partitionBy("day")
        .mode("append").save(s"$s3TableLocation")


      // https://docs.delta.io/latest/presto-integration.html
      spark.sql(s"GENERATE symlink_format_manifest FOR TABLE delta.`$s3TableLocation`")
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName LOCATION 's3a://${TargetSystem.DELTA.toString}/$databaseName.db'".stripMargin)
      spark.sql(
        s"""CREATE EXTERNAL TABLE IF NOT EXISTS $databaseName.$tableName($tableCols)
            USING DELTA
            PARTITIONED BY(day)
            LOCATION '$s3TableLocation'""".stripMargin)
      spark.sql(s"ALTER TABLE delta.`$s3TableLocation` SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)")

//      spark.sql(s"GENERATE symlink_format_manifest FOR TABLE delta.`s3a://${TargetSystem.DELTA.toString}/$databaseName.db/$tableName`")
//      spark.sql(
//        s"""
//           |CREATE EXTERNAL TABLE $databaseName.$tableName ($tableCols)
//           |PARTITIONED BY (day)
//           |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
//           |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
//           |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
//           |LOCATION '<path-to-delta-table>/_symlink_format_manifest/'  -- location of the generated manifest
//           |""".stripMargin)

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
