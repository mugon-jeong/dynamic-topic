package net.musma.dandi.dynamictopic.domain

import org.springframework.data.jpa.repository.JpaRepository

interface KafkaSubscriptionRepository : JpaRepository<KafkaSubscription, Long> {
    fun findByTopic(topic: String): List<KafkaSubscription>
    fun findByTopicAndGroupId(topic: String, groupId: String): KafkaSubscription?
}