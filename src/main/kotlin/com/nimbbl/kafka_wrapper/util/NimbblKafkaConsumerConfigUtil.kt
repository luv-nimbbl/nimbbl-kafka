package com.nimbbl.kafka_wrapper.util

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import com.nimbbl.kafka_wrapper.constant.NimbblKafkaConfigLevel
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.*

object NimbblKafkaConsumerConfigUtil {

    /**
     * Returns Kafka Consumer properties based on the specified priority level.
     * Each level defines tuning for reliability, performance, and throughput.
     */
    fun getConfigLevelProps(
        bootstrapServers: String,
        groupId: String,
        schemaRegistryUrl: String,
        clientId: String,
        kafkaConfigLevel: NimbblKafkaConfigLevel
    ): Properties {
        val baseProps = buildBaseProps(bootstrapServers, groupId, schemaRegistryUrl, clientId)
        return when (kafkaConfigLevel) {
            NimbblKafkaConfigLevel.LOW -> lowPriorityProps(baseProps)
            NimbblKafkaConfigLevel.MEDIUM -> mediumPriorityProps(baseProps)
            NimbblKafkaConfigLevel.HIGH -> highPriorityProps(baseProps)
        }
    }

    /**
     * Base properties shared across all priority levels.
     * Includes connection details, deserialization, and schema settings.
     */
    private fun buildBaseProps(
        bootstrapServers: String,
        groupId: String,
        schemaRegistryUrl: String,
        clientId: String
    ): Properties {
        return Properties().apply {
            // Deserialization
            this[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
            this[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java.name

            // Connection
            this[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
            this[ConsumerConfig.GROUP_ID_CONFIG] = groupId
            this[ConsumerConfig.CLIENT_ID_CONFIG] = clientId

            // Schema Registry
            this[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryUrl
            this["specific.avro.reader"] = true

            // Offset management
            this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

            // Common settings
            this[ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG] = 540000 // 9 minutes
            this[ConsumerConfig.METADATA_MAX_AGE_CONFIG] = 300000 // 5 minutes
            this[ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG] = 50
            this[ConsumerConfig.RETRY_BACKOFF_MS_CONFIG] = 100
            this[ConsumerConfig.CHECK_CRCS_CONFIG] = true // Data integrity check
        }
    }

    /**
     * Consumer configuration for low-priority use cases.
     * Optimized for simple consumption patterns with basic reliability.
     * Suitable for non-critical data processing, logging, or monitoring.
     */
    private fun lowPriorityProps(baseProps: Properties): Properties = baseProps.apply {
        // Auto-commit for simplicity
        this[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = true
        this[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG] = 5000

        // Fetch settings (smaller batches, longer waits)
        this[ConsumerConfig.FETCH_MIN_BYTES_CONFIG] = 1
        this[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG] = 500
        this[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 100
        this[ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG] = 1 * 1024 * 1024 // 1MB

        // Session management (relaxed)
        this[ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG] = 30000 // 30 seconds
        this[ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG] = 10000 // 10 seconds
        this[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = 300000 // 5 minutes

        // Timeout settings
        this[ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG] = 30000 // 30 seconds
        this[ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG] = 60000 // 1 minute

        // Isolation level
        this[ConsumerConfig.ISOLATION_LEVEL_CONFIG] = "read_uncommitted"
    }

    /**
     * Consumer configuration for medium-priority use cases.
     * Balanced between performance and reliability.
     * Suitable for business-critical applications with moderate throughput requirements.
     */
    private fun mediumPriorityProps(baseProps: Properties): Properties = baseProps.apply {
        // Manual commit for better control
        this[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false

        // Fetch settings (moderate batches)
        this[ConsumerConfig.FETCH_MIN_BYTES_CONFIG] = 1024
        this[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG] = 300
        this[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 500
        this[ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG] = 2 * 1024 * 1024 // 2MB

        // Session management (balanced)
        this[ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG] = 20000 // 20 seconds
        this[ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG] = 6000 // 6 seconds (1/3 of session timeout)
        this[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = 300000 // 5 minutes

        // Timeout settings
        this[ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG] = 40000 // 40 seconds
        this[ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG] = 120000 // 2 minutes

        // Isolation level for better consistency
        this[ConsumerConfig.ISOLATION_LEVEL_CONFIG] = "read_committed"

        // Partition assignment strategy
        this[ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG] = "org.apache.kafka.clients.consumer.RangeAssignor"
    }

    /**
     * Consumer configuration for high-priority use cases.
     * Optimized for maximum reliability, throughput, and fine-grained control.
     * Suitable for mission-critical applications requiring strict processing guarantees.
     */
    private fun highPriorityProps(baseProps: Properties): Properties = baseProps.apply {
        // Manual commit for maximum control
        this[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false

        // Fetch settings (larger batches for throughput)
        this[ConsumerConfig.FETCH_MIN_BYTES_CONFIG] = 4096
        this[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG] = 100 // Low latency
        this[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 1000 // Higher throughput
        this[ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG] = 4 * 1024 * 1024 // 4MB

        // Session management (tighter control)
        this[ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG] = 15000 // 15 seconds
        this[ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG] = 3000 // 3 seconds (1/5 of session timeout)
        this[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = 180000 // 3 minutes (shorter for faster rebalancing)

        // Timeout settings (longer for reliability)
        this[ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG] = 60000 // 60 seconds
        this[ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG] = 180000 // 3 minutes

        // Isolation level for strict consistency
        this[ConsumerConfig.ISOLATION_LEVEL_CONFIG] = "read_committed"

        // Advanced partition assignment for better load balancing
        this[ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG] = listOf(
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor",
            "org.apache.kafka.clients.consumer.RangeAssignor"
        )

        // Security and reliability settings
        this[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = false
        this[ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG] = true

        // Interceptors for monitoring (if needed)
        // this[ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG] = "your.monitoring.ConsumerInterceptor"
    }
}