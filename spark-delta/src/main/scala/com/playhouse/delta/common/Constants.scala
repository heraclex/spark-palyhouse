package com.playhouse.delta.common

object Constants {
  val hiveMetastore = "thrift://127.0.0.1:9083"
  val hiveWareHouse = "s3a://hive/"
  val sparkLogDirectory = "s3a://spark/log"
  val sparkCheckpointDirectory = "s3a://spark/checkpoint"
  val s3aEndpoint = "http://127.0.0.1:9000"
  val s3aAccessKey = "spark"
  val s3aSecretKey = "spark12345"

  val databaseName = "travel"
  val tableName = "hotel"

  val postgresUser = "postgres"
  val postgresPass = "postgres"
}

object TargetSystem extends Enumeration {
  type TargetSystem = Value

  // https://docs.amazonaws.cn/en_us/redshift/latest/mgmt/configure-jdbc-connection.html
  val DELTA = Value("delta")
  val HIVE = Value("hive")
}
