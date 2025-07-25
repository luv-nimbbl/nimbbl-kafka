package com.nimbbl.kafka_wrapper.wrapper

import com.nimbbl.avro.payload.NimbblKafkaPayload
import com.nimbbl.kafka_wrapper.constant.EnvVariables
import com.nimbbl.kafka_wrapper.constant.NimbblKafkaConfigLevel
import com.nimbbl.kafka_wrapper.exception.NimbblKafkaInvalidTopicException
import com.nimbbl.kafka_wrapper.exception.NimbblKafkaWrapperBuildException
import com.nimbbl.kafka_wrapper.util.AppLogger
import com.nimbbl.kafka_wrapper.util.NimbblKafkaProducerConfigUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.*
import org.slf4j.LoggerFactory

class NimbblKafkaProducerWrapper private constructor(
    private val producer: KafkaProducer<String, NimbblKafkaPayload>,
    private val validTopics: List<String>,
) {
    private val dlqTopic = "dlq"
    private val logger = AppLogger.getLogger(this::class.java)

    fun send(nimbblKafkaPayload: NimbblKafkaPayload) {
        val topic = nimbblKafkaPayload.topic
        val key = nimbblKafkaPayload.key

        if (!validTopics.contains(topic)) {
            throw NimbblKafkaInvalidTopicException("Invalid topic. Not configured in build: $topic")
        }

        val record = ProducerRecord(topic, key, nimbblKafkaPayload)

        producer.send(record) { metadata, exception ->
            if (exception != null) {
                logger.error("Failed to send to topic=$topic, key=$key: ${exception.message}", exception)
                sendToDlq(nimbblKafkaPayload)
            } else {
                logger.debug("Sent to topic=${metadata.topic()}, partition=${metadata.partition()}, offset=${metadata.offset()}")
            }
        }
    }

    fun sendToDlq(nimbblKafkaPayload: NimbblKafkaPayload) {
        val key = nimbblKafkaPayload.key
        val record = ProducerRecord(dlqTopic, key, nimbblKafkaPayload)
        producer.send(record) { metadata, exception ->
            if (exception != null) {
                logger.error("Failed to send to DLQ topic=$dlqTopic, key=$key: ${exception.message}", exception)
            } else {
                logger.debug("DLQ message sent to topic=${metadata.topic()}, partition=${metadata.partition()}, offset=${metadata.offset()}")
            }
        }
    }

    fun sendToDlq(record: ConsumerRecord<String, NimbblKafkaPayload>) {
        val key = record.key()
        val payload = record.value()
        if (payload == null) {
            logger.warn("Null payload encountered while sending to DLQ, topic=${record.topic()}, offset=${record.offset()}")
            return
        }

        val dlqRecord = ProducerRecord(dlqTopic, key, payload)
        producer.send(dlqRecord) { metadata, exception ->
            if (exception != null) {
                logger.error("Failed to send to DLQ topic=$dlqTopic, key=$key: ${exception.message}", exception)
            } else {
                logger.debug("DLQ-Wrapper message sent to topic=${metadata.topic()}, partition=${metadata.partition()}, offset=${metadata.offset()}")
            }
        }
    }

    fun flush() = producer.flush()

    fun close() {
        try {
            producer.close()
            logger.info("Kafka producer closed successfully")
        } catch (e: Exception) {
            logger.error("Error while closing Kafka producer", e)
        }
    }

    class Builder {
        private val builderLogger = LoggerFactory.getLogger(this::class.java)

        private var topics = emptyList<String>()
        private var configLevel: NimbblKafkaConfigLevel? = null
        private var kafkaBootstrapServers = EnvVariables.KAFKA_BOOTSTRAP_SERVERS
        private var serviceId = EnvVariables.SERVICE_ID
        private var serviceName = EnvVariables.SERVICE_NAME
        private var schemaRegistryUrl = EnvVariables.SCHEMA_REGISTRY_URL
        private var retryCount = 3

        fun setTopics(topics: List<String>) = apply {
            this.topics = topics
        }

        fun setTopic(topic: String) = apply {
            this.topics = listOf(topic)
        }

        fun setRetryCount(retryCount: Int) = apply {
            this.retryCount = retryCount
        }

        fun setKafkaBootstrapServers(value: String) = apply {
            this.kafkaBootstrapServers = value
        }

        fun setServiceId(value: String) = apply {
            this.serviceId = value
        }

        fun setServiceName(value: String) = apply {
            this.serviceName = value
        }

        fun setSchemaRegistryUrl(value: String) = apply {
            this.schemaRegistryUrl = value
        }



        fun setNimbblKafkaConfigLevel(nimbblKafkaConfigLevel: NimbblKafkaConfigLevel) = apply {
            this.configLevel = nimbblKafkaConfigLevel
        }


        fun build(): NimbblKafkaProducerWrapper {
            val validationErrors = validate()
            if (validationErrors.isNotEmpty()) {
                builderLogger.error("KafkaProducerWrapper validation failed: {}", validationErrors)
                throw NimbblKafkaWrapperBuildException("Invalid KafkaProducerWrapper config: $validationErrors")
            }
            val validatedConfigLevel = configLevel!!
            val configLevelProps = NimbblKafkaProducerConfigUtil.getConfigLevelProps(
                bootstrapServers = kafkaBootstrapServers,
                schemaRegistryUrl = schemaRegistryUrl,
                clientId = "$serviceName-$serviceId",
                kafkaConfigLevel = validatedConfigLevel
            )
            val producer = KafkaProducer<String, NimbblKafkaPayload>(configLevelProps)

            return NimbblKafkaProducerWrapper(producer, topics)
        }

        private fun validate(): Map<String, String> {
            val errors = mutableMapOf<String, String>()
            if (topics.isEmpty()) errors["topics"] = "At least one topic must be provided"
            if (topics.any { it.isBlank() }) errors["topics"] = "Topic names must not be blank"
            if (kafkaBootstrapServers.isBlank()) errors["kafkaBootstrapServers"] = "Kafka bootstrap servers must be set or passed from environment variable: NIMBBL_KAFKA_BOOTSTRAP_SERVERS"
            if (serviceId.isBlank()) errors["serviceId"] = "Service ID must be set or passed from environment variable: SERVICE_ID"
            if (serviceName.isBlank()) errors["serviceName"] = "Service name must be set or passed from environment variable: SERVICE_NAME"
            if (schemaRegistryUrl.isBlank()) errors["schemaRegistryUrl"] = "Schema registry URL must be set or passed from environment variable: NIMBBL_KAFKA_SCHEMA_REGISTRY_URL"
            if(retryCount > 5) errors["retryCount"] = "Retry count must be <= 5"
            if (configLevel == null) errors["configLevel"] = "Kafka config level must be set via setNimbblKafkaConfigLevel()"
            return errors
        }
    }
}
