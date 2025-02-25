package net.musma.dandi.dynamictopic.kafka

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "spring.kafka")
data class KafkaProperties(
    val bootstrapServers: String,
    val properties: Properties = Properties(),
    val consumer: ConsumerProperties = ConsumerProperties(),
    val producer: ProducerProperties = ProducerProperties(),
    val listener: ListenerProperties = ListenerProperties()
) {
    data class Properties(
        val request: Request = Request(),
        val connections: Connections = Connections()
    ) {
        data class Request(val timeoutMs: String = "60000")
        data class Connections(val maxIdleMs: String = "600000")
    }

    data class ConsumerProperties(
        val autoOffsetReset: String = "latest",
        val enableAutoCommit: Boolean = true,
        val keyDeserializer: String = "org.apache.kafka.common.serialization.StringDeserializer",
        val valueDeserializer: String = "org.apache.kafka.common.serialization.StringDeserializer",
        val groupId: String = "pipeline",
    )

    data class ProducerProperties(
        val acks: String = "all",
        val retries: Int = 3,
        val keySerializer: String = "org.apache.kafka.common.serialization.StringSerializer",
        val valueSerializer: String = "org.apache.kafka.common.serialization.StringSerializer"
    )

    data class ListenerProperties(
        val missingTopicsFatal: Boolean = false
    )
}