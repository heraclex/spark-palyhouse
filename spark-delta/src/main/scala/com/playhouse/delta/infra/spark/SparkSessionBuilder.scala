package com.playhouse.delta.infra.spark

import com.playhouse.delta.infra.spark.SparkSessionDecorators.{SparkBuilderDecorator, SparkSessionDecorator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkSessionBuilder {
  def buildSparkSession(appName: String): SparkSession = {

    val sparkConf = new SparkConf()

    SparkSession
      .builder()
      .master(sparkConf.getOption("spark.master").getOrElse("local[*]"))
      .appName(appName)
      .sparkDefault
      .configHive()
      .getOrCreate()
      .configS3()
      .configLog(level = "INFO")
  }

  def build(appName: String = "NoAppName") = buildSparkSession(appName)
}

object SparkSessionBuilder {
  def apply(): SparkSessionBuilder = new SparkSessionBuilder()
}