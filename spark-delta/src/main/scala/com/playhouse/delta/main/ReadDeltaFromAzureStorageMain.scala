package com.playhouse.delta.main

import com.playhouse.delta.common.Constants.azureBlobStoreAccount
import com.playhouse.delta.infra.spark.SparkSessionBuilder
import com.playhouse.delta.sparkAppLogger
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object ReadDeltaFromAzureStorageMain {
  def main(args: Array[String]): Unit = {
    Try{
      implicit val spark: SparkSession = SparkSessionBuilder().build(appName = "spark-read-azure-delta-table")
      sparkAppLogger.info("Got spark...")

      val containerName = "gold"
      val tableName = "dim_vehicle"

      val path = s"wasbs://$containerName@$azureBlobStoreAccount.blob.core.windows.net"

      val df = spark.read.load(s"$path/$tableName")
      df.show(10, true)
//      df.write.partitionBy("day").mode("overwrite").saveAsTable(s"travel.hotel_20210101")

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
