package com.playhouse.delta.main


import scala.util.{Failure, Success, Try}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, LongType, MapType, StringType, StructField, StructType}
import com.playhouse.delta.infra.spark.SparkSessionBuilder
import com.playhouse.delta.sensorDataLogger

object WriteDeltaMain {
  def main(args: Array[String]): Unit = {
    Try{
      implicit val spark: SparkSession = SparkSessionBuilder().build(appName = "spark-delta")
      sensorDataLogger.info("Got spark...")

      // https://github.com/delta-io/connectors/issues/71
      val df = spark.read.option("delimiter", ",")
        .option("header", "true").csv("./src/main/resources/hotels.csv")
      sensorDataLogger.info(s"${df.count()}")

      val tableCols = s"(travelCode string, userCode string,name string, place string, days string, price string,total string, date string)"

      val database = "delta"
      val tableName = "hotel"
      val s3DatabaseLocation = s"s3a://hive/$database.db/"
      val s3TableLocation = s"$s3DatabaseLocation/$tableName/"
      // https://docs.databricks.com/spark/2.x/spark-sql/language-manual/create-table.html
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $database LOCATION '$s3DatabaseLocation'")
      spark.sql(
        s"""CREATE TABLE IF NOT EXISTS $tableName$tableCols USING DELTA LOCATION '$s3TableLocation'
           |PARTITIONED BY (date)""".stripMargin)

      df.write.format("delta").partitionBy("date").saveAsTable(s"$database.$tableName")

      //
//      val database = "delta"
//      val tableName = "hotels"
//      val s3DatabaseLocation = s"s3a://hive/$database.db/"
//      val s3TableLocation = s"$s3DatabaseLocation/$tableName/"
//
//      spark.sql(s"CREATE DATABASE IF NOT EXISTS $database LOCATION '$s3DatabaseLocation'")
//      spark.sql(s"""CREATE EXTERNAL TABLE IF NOT EXISTS $database.$tableName
//          (travelCode string,userCode string,name string, place string, days string, price string,total string, date string)
//          PARTITIONED BY (date)
//          STORED BY 'io.delta.hive.DeltaStorageHandler'
//          LOCATION '$s3TableLocation'""".stripMargin)


//      val df = spark.readStream.table("delta.trivago")
//
//      // Start running the query that prints the running counts to the console
//      val query = df.writeStream
//        .outputMode("append")
//        .format("console")
//        .start()
//
//      query.awaitTermination()


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
