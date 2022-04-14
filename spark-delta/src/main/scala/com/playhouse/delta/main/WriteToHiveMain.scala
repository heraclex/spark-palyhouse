package com.playhouse.delta.main

import com.playhouse.delta.common.Constants._
import com.playhouse.delta.common.TargetSystem
import com.playhouse.delta.infra.spark.SparkSessionBuilder
import com.playhouse.delta.sensorDataLogger
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object WriteToHiveMain {
  def main(args: Array[String]): Unit = {
    Try{
      implicit val spark: SparkSession = SparkSessionBuilder().build(appName = "spark-delta")
      sensorDataLogger.info("Got spark...")

      val tableCols =
        s"""travelCode int, userCode int, name string, place string, stayingDays int,
           |price float, total float, `date` date, day int""".stripMargin

      val s3DatabaseLocation = s"s3a://${TargetSystem.HIVE.toString}/$databaseName.db"
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
      spark.sql(
        s"""CREATE DATABASE IF NOT EXISTS $databaseName
           |LOCATION 's3a://${TargetSystem.HIVE.toString}/$databaseName.db'""".stripMargin)
      df.write.partitionBy("day").mode("overwrite")
        .saveAsTable(s"$databaseName.$tableName")

      // create hive table
      //      spark.sql(
      //        s"""CREATE EXTERNAL TABLE IF NOT EXISTS $database.$tableName($tableCols)
      //            PARTITIONED BY (day)
      //            ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
      //            STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
      //            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
      //            LOCATION '$s3TableLocation/_symlink_format_manifest/'""".stripMargin)
      //
      //      spark.sql(
      //        s"""ALTER TABLE $database.$tableName
      //           |SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)""".stripMargin)

      // MSCK is used to recover the partitions that exist on the file system, but not registered in the metadata
      //      spark.sql(s"MSCK REPAIR TABLE $database.$tableName")

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
