package com.playhouse.delta.main

import com.playhouse.delta.common.Constants._
import com.playhouse.delta.common.TargetSystem
import com.playhouse.delta.infra.spark.SparkSessionBuilder
import com.playhouse.delta.sensorDataLogger
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object ReadDeltaMain {
  def main(args: Array[String]): Unit = {
    Try{
      implicit val spark: SparkSession = SparkSessionBuilder().build(appName = "spark-delta")
      sensorDataLogger.info("Got spark...")


      val location = s"s3a://${TargetSystem.DELTA.toString}/$databaseName.db/$tableName"
      val history = spark.sql(s"DESCRIBE HISTORY delta.`$location`")
      history.show(10, true)
      val hotel = spark.sql(s"select * from delta.`$location` where day > 20211010")


      hotel.show(10, true)




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
