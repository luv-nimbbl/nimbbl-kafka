package com.nimbbl.kafka_wrapper.util
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object AppLogger {

    fun getLogger(forClass: Class<*>): LoggerWrapper {
        val slf4jLogger = LoggerFactory.getLogger(forClass)
        return LoggerWrapper(slf4jLogger)
    }

    fun getLogger(name: String): LoggerWrapper {
        val slf4jLogger = LoggerFactory.getLogger(name)
        return LoggerWrapper(slf4jLogger)
    }

    class LoggerWrapper(private val logger: Logger) {

        fun info(msg: String) = logger.info(msg)
        fun debug(msg: String) = logger.debug(msg)
        fun warn(msg: String) = logger.warn(msg)
        fun error(msg: String) = logger.error(msg)
        fun trace(msg: String) = logger.trace(msg)

        // Overloads
        fun error(msg: String, throwable: Throwable) = logger.error(msg, throwable)
        fun warn(msg: String, throwable: Throwable) = logger.warn(msg, throwable)
    }
}
