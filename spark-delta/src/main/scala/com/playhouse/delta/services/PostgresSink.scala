package com.playhouse.delta.services

import com.playhouse.delta.sparkAppLogger
import com.typesafe.scalalogging.LazyLogging
import com.playhouse.delta.utils.TimeUtils

import java.sql.{Connection, Date, DriverManager}
import scala.util.{Failure, Success, Try}


case class TotalRevenuePerHotel(updateTime: Long, hotelName: String, date: Date, total: Double)

case class PostgresInstance(host: String, port: Int, userName: String, password: String, db: String)

abstract class RelationalDBConnector(userName: String, password: String, url: String, driveClz: String) {

  protected def getConnection: Try[Connection] = Try {
    Class.forName(driveClz)
    val connection = DriverManager.getConnection(url, userName, password)
    connection.setAutoCommit(false)
    connection
  }

}

sealed trait MetricSink extends LazyLogging {

  def process(commands: Seq[TotalRevenuePerHotel])

  protected def metricRetentionHours: Int

}

case class PostgresSink(postgres: PostgresInstance)
  extends RelationalDBConnector(
    userName = postgres.userName,
    password = postgres.password,
    url = s"jdbc:postgresql://${postgres.host}:${postgres.port}/${postgres.db}",
    driveClz = "org.postgresql.Driver"
) with MetricSink {

  override def process(commands: Seq[TotalRevenuePerHotel]): Unit = {
    val ts = commands.head.updateTime
    val start = System.currentTimeMillis
    getConnection.flatMap(implicit conn => {
      try {
        executeQueryInPostgres(Seq(createSchemaIfNotExistsSql))
          .flatMap(_ => executeQueryInPostgres(Seq(createMasterTableIfNotExistSql)))
          .flatMap(_ => executeQueryInPostgres(Seq(createIndexOnMasterTableSql(ts))))
          .flatMap(_ => executeQueryInPostgres(Seq(createPartitionTableSql(ts))))
          .flatMap(_ => executeQueryInPostgres(commands.map(createInsertCommands)))
          .flatMap(_ => executeQueryInPostgres(Seq(dropOldTableSqls(ts))))
          .flatMap(_ => Try { conn.commit() })
      } finally {
        conn.close()
      }
    }) match {
      case Success(_) =>
        logger.info(s"All Postgres queries took ${System.currentTimeMillis - start} ms")
      case Failure(ex) => ex.printStackTrace()
    }
  }

  override protected def metricRetentionHours: Int = 1 * 24

  def executeQueryInPostgres(sqls: Seq[String])(implicit conn: Connection): Try[Unit] = Try {
    sparkAppLogger.info(s"Executing sqls ...${sqls.mkString("\n")}")
    val start = System.currentTimeMillis()
    val stm = conn.createStatement()
    sqls.foreach(stm.addBatch)
    stm.executeBatch()
    sparkAppLogger.info(s"Executing ${sqls.size} sql(s) against Postgres took ${System.currentTimeMillis() - start} ms")
  }

  def df: Long => String = TimeUtils.formatTime("YYYYMMdd")

  val schemaName = "delta_to_postgres"
  val tableName = s"$schemaName.total_revenue_per_hotel"
  val indexName = "idx_total_revenue_per_hotel"

  private val createSchemaIfNotExistsSql = s"CREATE SCHEMA IF NOT EXISTS $schemaName"

  private val createMasterTableIfNotExistSql: String =
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName
       |(
       |    update_time  timestamptz not null,
       |    hotel_name  varchar(100),
       |    date        Date,
       |    total       float
       |) partition by RANGE (update_time)
    """.stripMargin

  private def createPartitionTableSql(ts: Long) = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${tableName}_${df(ts)}
       |  PARTITION OF $tableName
       |  FOR VALUES FROM ('${TimeUtils.formatTimeISO8061(TimeUtils.startTimeOfTheDay(ts))}')
       |  TO ('${TimeUtils.formatTimeISO8061(TimeUtils.startTimeOfTheDay(ts + TimeUtils.OneDayMillis))}')
       |""".stripMargin
  }

  private def createIndexOnMasterTableSql(ts: Long) =
    s"""
       |CREATE INDEX IF NOT EXISTS ${indexName}
       | ON $tableName(hotel_name, date DESC )
       |""".stripMargin

  private def createInsertCommands(input: TotalRevenuePerHotel): String =
    s"INSERT INTO $tableName VALUES ('${TimeUtils.formatTimeISO8061(input.updateTime)}','${input.hotelName}','${input.date}',${input.total})"

  private def dropOldTableSqls(ts: Long) = {
    val outdatedPartitionTs = ts - metricRetentionHours * TimeUtils.OneHourMillis
    s"DROP TABLE IF EXISTS ${tableName}_${df(outdatedPartitionTs)}"
  }
}
