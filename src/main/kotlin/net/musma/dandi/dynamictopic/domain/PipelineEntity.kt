package net.musma.dandi.dynamictopic.domain

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table

@Entity
@Table(name = "pipeline")
data class PipelineEntity(
    @Id
    val groupId: String, // ✅ 그룹 ID (Primary Key)

    @Column(columnDefinition = "TEXT") // ✅ JSON 형식으로 저장
    var topicsJson: String
) {
    companion object {
        private val objectMapper = jacksonObjectMapper()

        fun from(groupId: String, topics: List<String>): PipelineEntity {
            return PipelineEntity(groupId, objectMapper.writeValueAsString(topics))
        }
    }

    fun getTopics(): List<String> = objectMapper.readValue(topicsJson)
    fun setTopics(topics: List<String>) {
        topicsJson = objectMapper.writeValueAsString(topics)
    }
}