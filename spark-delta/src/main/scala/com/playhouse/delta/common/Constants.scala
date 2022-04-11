package com.playhouse.delta.common

object Constants {
  val hiveThriftServer = "thrift://127.0.0.1:9083"
  val hiveWareHouse = "s3a://hive/warehouse/"
  val sparkLogDirectory = "s3a://spark/log"
  val sparkCheckpointDirectory = "s3a://spark/checkpoint"
  val s3aEndpoint = "http://127.0.0.1:9000"
  val s3aAccessKey = "spark"
  val s3aSecretKey = "spark12345"
}
