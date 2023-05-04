package com.playhouse.delta.infra.spark

import com.playhouse.delta.common.Constants.azureBlobStoreAccount

import scala.io.Source
import com.playhouse.delta.infra.spark.SparkSessionDecorators.{SparkBuilderDecorator, SparkSessionDecorator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkSessionBuilder {

  def build(appName: String = "NoAppName") = buildSparkSession(appName)

  private def buildSparkSession(appName: String): SparkSession = {

    val sparkConf = new SparkConf()

    SparkSession
      .builder()
      .master(sparkConf.getOption("spark.master").getOrElse("local[*]"))
      .appName(appName)
      .sparkDefault
      .configHive()
      .getOrCreate()
      .configS3()
      .configAzureBlobStorage(azureBlobStoreAccount, getStorageAccountKey(azureBlobStoreAccount))
      .configLog(level = "INFO")
  }

  private def getStorageAccountKey(storageAccountName: String): String = {
    val map = Source.fromFile(s"/Users/${System.getProperty("user.name")}/.secrets/azure-blob-storage.key").getLines
      .map(_.split(":")).map(arr => arr(0) -> arr(1)).toMap

    map(storageAccountName)
  }
}

object SparkSessionBuilder {
  def apply(): SparkSessionBuilder = new SparkSessionBuilder()
}