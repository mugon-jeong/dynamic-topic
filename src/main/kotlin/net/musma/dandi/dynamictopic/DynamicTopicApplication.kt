package net.musma.dandi.dynamictopic

import net.musma.dandi.dynamictopic.kafka.KafkaProperties
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableConfigurationProperties(KafkaProperties::class)
class DynamicTopicApplication

fun main(args: Array<String>) {
    runApplication<DynamicTopicApplication>(*args)
}
