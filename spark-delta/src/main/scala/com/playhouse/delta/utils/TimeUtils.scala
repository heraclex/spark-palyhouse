package com.playhouse.delta.utils

import org.joda.time.{DateTime, DateTimeZone}

import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.TimeZone

object TimeUtils {

  val OneMinMillis: Long = 60 * 1000
  val OneHourMillis: Long = OneMinMillis * 60
  val OneDayMillis: Long = OneHourMillis * 24

  def formatTime(pattern: String)(timestamp: Long): String = {
    val df = DateTimeFormatter.ofPattern(pattern).withZone(ZoneId.systemDefault())
    df.format(Instant.ofEpochMilli(timestamp))
  }

  def formatTimeISO8061: Long => String = formatTime("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")

  def startTimeOfTheDay(ts: Long): Long = ts - new DateTime(ts).getMillisOfDay

  def getDateTimeWithGMT(unixTime: Long): ZonedDateTime = {
    val zoneId = ZoneId.of("GMT")
    if (unixTime.toString.length > 10) {
      Instant.ofEpochMilli(unixTime).atZone(zoneId)
    } else {
      Instant.ofEpochSecond(unixTime).atZone(zoneId)
    }
  }

  def getCurrentDateTimeWithGMT(): ZonedDateTime = {
    val zoneId = ZoneId.of("GMT")
    ZonedDateTime.now(zoneId)
  }

  def getDateTimeWithUTC(unixTime: Long): DateTime = {
    if (unixTime.toString.length > 10) {
      new DateTime(unixTime).withZone(DateTimeZone.UTC)
    } else {
      new DateTime(unixTime * 1000).withZone(DateTimeZone.UTC)
    }
  }

  def getDateTimeWithTimezone(unixTime: Long, country: String): DateTime = {
    val zoneId = DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT"))
    if (unixTime.toString.length > 10) {
      new DateTime(unixTime).withZone(zoneId)
    } else {
      new DateTime(unixTime * 1000).withZone(zoneId)
    }
  }
}
