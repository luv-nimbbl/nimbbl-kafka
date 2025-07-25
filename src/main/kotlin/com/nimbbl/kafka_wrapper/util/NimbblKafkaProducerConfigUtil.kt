package com.nimbbl.kafka_wrapper.util

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import com.nimbbl.kafka_wrapper.constant.NimbblKafkaConfigLevel
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

object NimbblKafkaProducerConfigUtil {

    /**
     * Returns Kafka Producer properties based on the specified priority level.
     */
    fun getConfigLevelProps(
        bootstrapServers: String,
        schemaRegistryUrl: String,
        clientId: String,
        kafkaConfigLevel: NimbblKafkaConfigLevel
    ): Properties {
        val baseProps = buildBaseProps(bootstrapServers, schemaRegistryUrl, clientId)
        return when (kafkaConfigLevel) {
            NimbblKafkaConfigLevel.LOW -> lowPriorityProps(baseProps)
            NimbblKafkaConfigLevel.MEDIUM -> mediumPriorityProps(baseProps)
            NimbblKafkaConfigLevel.HIGH -> highPriorityProps(baseProps)
        }
    }

    /**
     * Base properties shared across all priority levels.
     * Includes connection details, serialization, and schema settings.
     */
    private fun buildBaseProps(
        bootstrapServers: String,
        schemaRegistryUrl: String,
        clientId: String
    ): Properties {
        return Properties().apply {
            // Serialization
            this[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
            this[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java.name

            // Connection
            this[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
            this[ProducerConfig.CLIENT_ID_CONFIG] = clientId

            // Schema Registry
            this[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryUrl
            this[AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS] = true

            // Common settings
            this[ProducerConfig.COMPRESSION_TYPE_CONFIG] = "snappy"
            this[ProducerConfig.MAX_REQUEST_SIZE_CONFIG] = 1048576 // 1MB
            this[ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG] = 540000 // 9 minutes
            this[ProducerConfig.METADATA_MAX_AGE_CONFIG] = 300000 // 5 minutes
        }
    }

    /**
     * Producer configuration for low-priority use cases.
     * Optimized for throughput and batching efficiency.
     * Acceptable for non-critical data with some data loss tolerance.
     */
    private fun lowPriorityProps(baseProps: Properties): Properties = baseProps.apply {
        // Reliability settings (lower)
        this[ProducerConfig.RETRIES_CONFIG] = 1
        this[ProducerConfig.ACKS_CONFIG] = "1"
        this[ProducerConfig.RETRY_BACKOFF_MS_CONFIG] = 100

        // Performance settings (optimized for throughput)
        this[ProducerConfig.BUFFER_MEMORY_CONFIG] = 32 * 1024 * 1024 // 32MB
        this[ProducerConfig.LINGER_MS_CONFIG] = 100
        this[ProducerConfig.BATCH_SIZE_CONFIG] = 16 * 1024 // 16KB

        // Timeout settings
        this[ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG] = 15000 // 15 seconds
        this[ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG] = 30000 // 30 seconds

        // No idempotence for better performance
        this[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = false
    }

    /**
     * Producer configuration for medium-priority use cases.
     * Balanced approach between performance and reliability.
     * Suitable for business-critical data with moderate durability requirements.
     */
    private fun mediumPriorityProps(baseProps: Properties): Properties = baseProps.apply {
        // Reliability settings (balanced)
        this[ProducerConfig.RETRIES_CONFIG] = 3
        this[ProducerConfig.ACKS_CONFIG] = "all"
        this[ProducerConfig.RETRY_BACKOFF_MS_CONFIG] = 500

        // Performance settings (balanced)
        this[ProducerConfig.BUFFER_MEMORY_CONFIG] = 64 * 1024 * 1024 // 64MB
        this[ProducerConfig.LINGER_MS_CONFIG] = 20
        this[ProducerConfig.BATCH_SIZE_CONFIG] = 32 * 1024 // 32KB

        // Timeout settings
        this[ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG] = 20000 // 20 seconds
        this[ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG] = 60000 // 60 seconds

        // Enable idempotence for better reliability
        this[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = true

        // Max in-flight requests (compatible with idempotence)
        this[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION] = 5
    }

    /**
     * Producer configuration for high-priority use cases.
     * Optimized for maximum reliability, ordering, and idempotence.
     * Suitable for mission-critical data where data loss is unacceptable.
     */
    private fun highPriorityProps(baseProps: Properties): Properties = baseProps.apply {
        // Reliability settings (maximum)
        this[ProducerConfig.RETRIES_CONFIG] = Integer.MAX_VALUE
        this[ProducerConfig.ACKS_CONFIG] = "all"
        this[ProducerConfig.RETRY_BACKOFF_MS_CONFIG] = 1000

        // Performance settings (reliability over speed)
        this[ProducerConfig.BUFFER_MEMORY_CONFIG] = 128 * 1024 * 1024 // 128MB
        this[ProducerConfig.LINGER_MS_CONFIG] = 0 // Send immediately
        this[ProducerConfig.BATCH_SIZE_CONFIG] = 64 * 1024 // 64KB

        // Timeout settings (longer for reliability)
        this[ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG] = 30000 // 30 seconds
        this[ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG] = 300000 // 5 minutes

        // Idempotence and ordering guarantees
        this[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = true
        this[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION] = 1 // Strict ordering

        // Additional reliability settings
        this[ProducerConfig.TRANSACTION_TIMEOUT_CONFIG] = 60000 // 60 seconds (if using transactions)
    }
}