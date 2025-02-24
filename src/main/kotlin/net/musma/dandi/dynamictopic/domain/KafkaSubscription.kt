package net.musma.dandi.dynamictopic.domain

import jakarta.persistence.*

@Entity
@Table(name = "kafka_subscriptions")
data class KafkaSubscription(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(nullable = false)
    val topic: String,

    @Column(nullable = false)
    val groupId: String
)