package com.playhouse.delta.services

import com.typesafe.scalalogging.LazyLogging


case class ActiveUserCount(updateTime: Long, country: String, platform: String, activeUsers: Long, activeShoppers: Long)

case class PostgresInstance(host: String, port: Int, userName: String, password: String, db: String)

sealed trait StructuredStreamingSink extends LazyLogging {

  def process(commands: Seq[ActiveUserCount])

  protected def metricRetentionHours: Int

}
class PostgresSink {

}
