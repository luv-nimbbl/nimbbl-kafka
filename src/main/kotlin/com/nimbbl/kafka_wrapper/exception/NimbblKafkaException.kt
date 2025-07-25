package com.nimbbl.kafka_wrapper.exception

data class DeserializationException(override val message: String) : Exception()

data class NimbblKafkaInvalidTopicException(override val message: String = "Invalid topic") : IllegalArgumentException()

data class NimbblKafkaWrapperBuildException(override val message: String) : RuntimeException()
