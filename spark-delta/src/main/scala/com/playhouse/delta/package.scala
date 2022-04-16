package com.playhouse

import com.typesafe.scalalogging.LazyLogging

package object delta extends LazyLogging{
  class SparkAppLogger() {

    def info(msg: String) = {
      logger.info(msg)
    }
    def error(msg: String) = {
      logger.error(msg)
    }
    def debug(msg: String) = {
      logger.debug(msg)
    }
    def warn(msg: String) = {
      logger.warn(msg)
    }

  }
  lazy val sparkAppLogger = new SparkAppLogger()
}
