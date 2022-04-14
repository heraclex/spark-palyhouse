package com.playhouse.delta.infra.spark

import com.playhouse.delta.common.Constants._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder

private[spark] object SparkSessionDecorators {

  implicit class SparkSessionDecorator(spark: SparkSession) {
    def configS3() = {
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3aEndpoint)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3aAccessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3aSecretKey)

      spark.sparkContext.hadoopConfiguration.set("spark.sql.debug.maxToStringFields", "100")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")


//      val hadoopConfig = spark.sparkContext.hadoopConfiguration
//      spark.conf.set("spark.hadoop.fs.s3a.multiobjectdelete.enable", "true")
//      spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
//      spark.conf.set("spark.hadoop.fs.s3a.endpoint", s3aEndpoint)
////      spark.conf.set("spark.hadoop.fs.s3a.access.key", s3aAccessKey)
////      spark.conf.set("spark.hadoop.fs.s3a.secret.key", s3aSecretKey)
//      spark.conf.set("spark.hadoop.fs.s3a.path.style.access", true)
//
////      spark.conf.set("fs.s3a.multiobjectdelete.enable", "true")
////      spark.conf.set("fs.s3a.fast.upload", "true")
////      spark.conf.set("fs.s3a.endpoint", s3aEndpoint)
//      spark.conf.set("fs.s3a.access.key", s3aAccessKey)
//      spark.conf.set("fs.s3a.secret.key", s3aSecretKey)
//      spark.conf.set("fs.s3a.path.style.access", true)
//
//
//      // hadoopConfig.set("fs.s3a.aws.credentials.provider", classOf[org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider].getName)
//      //spark.conf.set("fs.s3a.aws.credentials.provider", classOf[DefaultAWSCredentialsProviderChain].getName)
//      // hadoopConfig.set("fs.s3a.impl", classOf[org.apache.hadoop.fs.s3a.S3AFileSystem].getName)
//      spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//      //spark.conf.set("fs.s3a.aws.credentials.provider", new BasicAWSCredentials(s3aAccessKey, s3aSecretKey))
//      //spark.conf.set("fs.s3a.aws.credentials.provider", classOf[BasicAWSCredentials].getName)
//
//      //spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      spark
    }

    def configLog(level: String) = {
      spark.sparkContext.setLogLevel(level)
      spark
    }
  }

  implicit class SparkBuilderDecorator(b: Builder) {

    def sparkDefault: SparkSession.Builder = {
      b
        .config("spark.sql.broadcastTimeout", 3600)
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
        .config("spark.executor.heartbeatInterval", 180000)
        .config("spark.network.timeout", 2000000)
        .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")

        .config("spark.history.fs.logDirectory", sparkLogDirectory)
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    }

    def configCheckPoint(checkPointEnabled: Boolean): SparkSession.Builder =
      b.config("checkpointLocation", checkPointEnabled)

    def configHive(): SparkSession.Builder = {
      b.config("spark.network.timeout", "10000s")
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("hive.metastore.uris", hiveMetastore)
        .config("spark.sql.warehouse.dir", hiveWareHouse)
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.history.fs.logDirectory", sparkLogDirectory)
        .config("spark.ui.enabled", "true")
        .enableHiveSupport()
    }
  }
}