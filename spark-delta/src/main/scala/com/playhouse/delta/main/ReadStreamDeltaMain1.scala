package com.playhouse.delta.main

import com.playhouse.delta.common.Constants.{databaseName, sparkCheckpointDirectory, tableName}
import com.playhouse.delta.common.TargetSystem
import com.playhouse.delta.infra.spark.SparkSessionBuilder
import com.playhouse.delta.sensorDataLogger
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object ReadStreamDeltaMain1 {
  def main(args: Array[String]): Unit = {
    Try{
      implicit val spark: SparkSession = SparkSessionBuilder().build(appName = "spark-delta")
      sensorDataLogger.info("Got spark...")

      spark.readStream.format("delta").table(s"$databaseName.hotel").createTempView("tmp")

      val df = spark.sql(
        s"""select * from tmp
           |where place in ('Natal (RN)', 'Sao Paulo (SP)', 'Rio de Janeiro (RJ)')""".stripMargin)

      val query = df.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("day")
        .option("checkpointLocation", s"$sparkCheckpointDirectory/$databaseName/hotel1")
        .toTable(s"$databaseName.hotel1")
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
