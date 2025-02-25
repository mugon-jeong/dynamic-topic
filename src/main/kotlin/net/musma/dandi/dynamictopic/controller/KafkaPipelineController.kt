package net.musma.dandi.dynamictopic.controller

import net.musma.dandi.dynamictopic.domain.PipelineEntity
import net.musma.dandi.dynamictopic.kafka.DynamicKafkaConsumerService
import net.musma.dandi.dynamictopic.kafka.KafkaPipelineService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/pipelines")
class KafkaPipelineController(
    private val kafkaPipelineService: KafkaPipelineService,
    private val dynamicKafkaConsumerService: DynamicKafkaConsumerService
) {

    @GetMapping("/consumers")
    fun getAllConsumerStatus(): ResponseEntity<Map<String, Map<String, Boolean>>> {
        val originalMap = dynamicKafkaConsumerService.getConsumerStatus()

        // ConsumerKey(groupId, topic)를 기반으로 새로운 맵 생성
        val transformedMap = originalMap.entries
            .groupBy({ it.key.groupId }, { it.key.topic to it.value }) // groupId 기준으로 묶기
            .mapValues { entry -> entry.value.toMap() } // topicId -> Boolean Map으로 변환

        return ResponseEntity.ok(transformedMap)
    }

    /**
     * ✅ 파이프라인 등록 (POST /pipelines)
     */
    @PostMapping
    fun registerPipeline(@RequestBody request: PipelineRequest): PipelineEntity {
        return kafkaPipelineService.registerPipeline(request.groupId,request.rootTopic, request.topicRelations)
    }

    /**
     * ✅ 특정 파이프라인 실행 (POST /pipelines/{groupId}/run)
     * Body에 메시지를 포함하여 첫 번째 토픽에 전송
     */
    @PostMapping("/{groupId}/run")
    fun runPipeline(@PathVariable groupId: String, @RequestBody request: PipelineRunRequest): ResponseEntity<String> {
        kafkaPipelineService.runPipeline(groupId, request.message)
        return ResponseEntity.ok("🚀 파이프라인 실행 시작 (groupId: $groupId, message: ${request.message})")
    }

    /**
     * ✅ 모든 파이프라인 목록 조회 (GET /pipelines)
     */
    @GetMapping
    fun getAllPipelines(): ResponseEntity<MutableList<PipelineEntity>> {
        return ResponseEntity.ok(kafkaPipelineService.getAllPipelines())
    }

    /**
     * ✅ 특정 파이프라인 삭제 (DELETE /pipelines/{groupId})
     */
    @DeleteMapping("/{groupId}")
    fun deletePipeline(@PathVariable groupId: String): ResponseEntity<String> {
        kafkaPipelineService.deletePipeline(groupId)
        return ResponseEntity.ok("🗑️ 파이프라인 삭제 완료 (groupId: $groupId)")
    }

    /**
     * ✅ 파이프라인 등록 요청 DTO
     */
    data class PipelineRequest(
        val groupId: String,
        val rootTopic: String,
        val topicRelations: Map<String, List<String>>
    )

    /**
     * ✅ 파이프라인 실행 요청 DTO
     */
    data class PipelineRunRequest(
        val message: String
    )
}