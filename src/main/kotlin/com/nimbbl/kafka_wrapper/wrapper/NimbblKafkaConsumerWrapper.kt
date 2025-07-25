package com.nimbbl.kafka_wrapper.wrapper

import com.nimbbl.avro.payload.NimbblKafkaPayload
import com.nimbbl.kafka_wrapper.constant.EnvVariables
import com.nimbbl.kafka_wrapper.constant.NimbblKafkaConfigLevel
import com.nimbbl.kafka_wrapper.exception.NimbblKafkaWrapperBuildException
import com.nimbbl.kafka_wrapper.util.AppLogger
import com.nimbbl.kafka_wrapper.util.NimbblKafkaConsumerConfigUtil
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.time.Duration

class NimbblKafkaConsumerWrapper private constructor(
    private val consumer: KafkaConsumer<String, NimbblKafkaPayload>,
    private val nimbblKafkaConfigLevel: NimbblKafkaConfigLevel,
    private val pollDuration: Duration,
    private val kafkaProducerWrapper: NimbblKafkaProducerWrapper,
    private val retryCount : Int
) {


    private val logger = AppLogger.getLogger(this::class.java)

    fun poll(): List<NimbblKafkaPayload> {
        val resultList = mutableListOf<NimbblKafkaPayload>()
        try {
            val records = consumer.poll(pollDuration)
            if (records.isEmpty) {
                logger.debug("No records polled.")
                return resultList
            }

            for (record in records) {
                try {
                    val payload = record.value()
                    resultList.add(payload)
                } catch (e: Exception) {
                    logger.warn(
                        "Initial processing failed for topic=${record.topic()}, partition=${record.partition()}, offset=${record.offset()}, key=${record.key()}: ${e.message}",
                        e
                    )
                    processWithRetry(record, resultList)
                }
            }
            logger.info("Total records polled: ${records.count()}")
            if(shouldManualCommit()) consumer.commitSync()
        } catch (e: Exception) {
            logger.error("Polling failed: {}",e)
        }
        return resultList
    }

    fun sendToDlq(record : NimbblKafkaPayload){
        kafkaProducerWrapper.sendToDlq(record)
    }

    private fun shouldManualCommit() : Boolean{
        return nimbblKafkaConfigLevel == NimbblKafkaConfigLevel.MEDIUM || nimbblKafkaConfigLevel == NimbblKafkaConfigLevel.HIGH
    }



    private fun processWithRetry(
        record: ConsumerRecord<String, NimbblKafkaPayload>,
        resultList: MutableList<NimbblKafkaPayload>
    ) {
        repeat(retryCount) { attempt ->
            try {
                val payload = record.value() ?: throw IllegalArgumentException("Null payload during retry")
                resultList.add(payload)
                logger.info("Retry successful on attempt ${attempt + 1} for record with key=${record.key()}")
                return
            } catch (e: Exception) {
                logger.warn(
                    "Retry attempt ${attempt + 1}/$retryCount failed for topic=${record.topic()}, partition=${record.partition()}, offset=${record.offset()}, key=${record.key()}: ${e.message}",
                    e
                )
                if (attempt == retryCount - 1) {
                    try {
                        kafkaProducerWrapper.sendToDlq(record)
                        logger.info("Record sent to DLQ: key=${record.key()}, topic=${record.topic()}")
                    } catch (dlqException: Exception) {
                        logger.error("Failed to send record to DLQ: ${dlqException.message}", dlqException)
                    }
                }
            }
        }
    }

    fun close() {
        try {
            consumer.close()
            logger.info("Kafka consumer closed.")
        } catch (e: Exception) {
            logger.error("Error closing Kafka consumer: ${e.message}", e)
        }
    }

    class Builder {
        private val logger = AppLogger.getLogger(this::class.java)

        private var pollDuration: Duration = Duration.ofMillis(1000)
        private var topics: List<String> = emptyList()
        private var configLevel: NimbblKafkaConfigLevel? = null
        private var producerWrapper: NimbblKafkaProducerWrapper? = null
        private var kafkaBootstrapServers = EnvVariables.KAFKA_BOOTSTRAP_SERVERS
        private var serviceId = EnvVariables.SERVICE_ID
        private var serviceName = EnvVariables.SERVICE_NAME
        private var consumerGroupId = EnvVariables.CONSUMER_GROUP_ID
        private var schemaRegistryUrl = EnvVariables.SCHEMA_REGISTRY_URL
        private var retryCount = 3

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

        fun setConsumerGroupId(value: String) = apply {
            this.consumerGroupId = value
        }

        fun setSchemaRegistryUrl(value: String) = apply {
            this.schemaRegistryUrl = value
        }



        fun setPollDuration(millis: Long) = apply {
            this.pollDuration = Duration.ofMillis(millis)
        }

        fun setTopics(topics: List<String>) = apply {
            this.topics = topics
        }

        fun setTopic(topic : String) = apply {
            this.topics = listOf(topic)
        }

        fun setNimbblKafkaConfigLevel(level: NimbblKafkaConfigLevel) = apply {
            this.configLevel = level
        }


        fun build(): NimbblKafkaConsumerWrapper {
            val errors = validate()
            if (errors.isNotEmpty()) {
                logger.error("Validation failed: $errors")
                throw NimbblKafkaWrapperBuildException("Invalid config: $errors")
            }
            val validatedConfigLevel = configLevel!!
            val configLevelProps = NimbblKafkaConsumerConfigUtil.getConfigLevelProps(
                bootstrapServers = kafkaBootstrapServers,
                groupId = consumerGroupId,
                schemaRegistryUrl = schemaRegistryUrl,
                clientId = "$serviceName-$serviceId",
                kafkaConfigLevel = validatedConfigLevel
            )
            val consumer = KafkaConsumer<String, NimbblKafkaPayload>(configLevelProps)
            consumer.subscribe(topics)

            // Produce to DLQ
            val dlqTopic = "dlq-wrapper"
            val producer = producerWrapper ?: NimbblKafkaProducerWrapper.Builder()
                .setNimbblKafkaConfigLevel(NimbblKafkaConfigLevel.LOW)
                .setTopics(listOf(dlqTopic))
                .build()

            return NimbblKafkaConsumerWrapper(consumer,validatedConfigLevel, pollDuration, producer,retryCount)
        }

        private fun validate(): Map<String, String> {
            val errors = mutableMapOf<String, String>()
            if (topics.isEmpty()) errors["topics"] = "At least one topic must be provided"
            if (topics.any { it.isBlank() }) errors["topics"] = "Topic names must not be blank"
            if (pollDuration.toMillis() <= 0) errors["pollDuration"] = "Poll duration must be > 0"
            if (kafkaBootstrapServers.isBlank()) errors["kafkaBootstrapServers"] = "Kafka bootstrap servers must be set or passed from environment variable: NIMBBL_KAFKA_BOOTSTRAP_SERVERS"
            if (serviceId.isBlank()) errors["serviceId"] = "Service ID must be set or passed from environment variable: SERVICE_ID"
            if (serviceName.isBlank()) errors["serviceName"] = "Service name must be set or passed from environment variable: SERVICE_NAME"
            if (consumerGroupId.isBlank()) errors["consumerGroupId"] = "Consumer group ID must be set or passed from environment variable: NIMBBL_KAFKA_CONSUMER_GROUP_ID"
            if (schemaRegistryUrl.isBlank()) errors["schemaRegistryUrl"] = "Schema registry URL must be set or passed from environment variable: NIMBBL_KAFKA_SCHEMA_REGISTRY_URL"
            if(retryCount > 5) errors["retryCount"] = "Retry count must be <= 5"
            if (configLevel == null) errors["configLevel"] = "Kafka config level must be set via setNimbblKafkaConfigLevel()"
            return errors
        }
    }
}
