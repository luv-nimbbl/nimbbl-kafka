package com.nimbbl.kafka_wrapper.constant

import com.nimbbl.kafka_wrapper.util.AppLogger
import java.util.UUID


private object EnvKeys {
    const val NIMBBL_KAFKA_BOOTSTRAP_SERVERS = "NIMBBL_KAFKA_BOOTSTRAP_SERVERS"
    const val SERVICE_ID =  "SERVICE_ID"
    const val SERVICE_NAME = "SERVICE_NAME"
    const val NIMBBL_KAFKA_CONSUMER_GROUP_ID = "NIMBBL_KAFKA_CONSUMER_GROUP_ID"
    const val NIMBBL_KAFKA_SCHEMA_REGISTRY_URL = "NIMBBL_KAFKA_SCHEMA_REGISTRY_URL"
}

private object EnvDefaultValues {
    private val data = HashMap<String, String>()

    init {
        data[EnvKeys.SERVICE_ID] = UUID.randomUUID().toString()
    }

    fun getDefaultValue(key: String): String? {
        return data[key]
    }
}

object EnvVariables {
    private val logger = AppLogger.getLogger(this::class.java)
    val KAFKA_BOOTSTRAP_SERVERS = getSystemEnv(EnvKeys.NIMBBL_KAFKA_BOOTSTRAP_SERVERS)
    val SERVICE_ID = getSystemEnv(EnvKeys.SERVICE_ID)
    val SERVICE_NAME = getSystemEnv(EnvKeys.SERVICE_NAME)
    val CONSUMER_GROUP_ID = getSystemEnv(EnvKeys.NIMBBL_KAFKA_CONSUMER_GROUP_ID)
    val SCHEMA_REGISTRY_URL = getSystemEnv(EnvKeys.NIMBBL_KAFKA_SCHEMA_REGISTRY_URL)

    private fun getSystemEnv(key: String): String {
        val value = System.getenv(key)
        if (value != null) {
            return value
        }
        logger.warn("Environment variable '$key' not found. Falling back to default value.")
        val defaultValue = EnvDefaultValues.getDefaultValue(key)
        if (defaultValue != null) {
            return defaultValue
        }
        return ""
    }
}
