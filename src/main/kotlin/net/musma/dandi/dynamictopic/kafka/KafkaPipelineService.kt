package net.musma.dandi.dynamictopic.kafka

import io.github.oshai.kotlinlogging.KotlinLogging
import net.musma.dandi.dynamictopic.domain.PipelineEntity
import net.musma.dandi.dynamictopic.domain.PipelineRepository
import org.springframework.stereotype.Service

private val logger = KotlinLogging.logger {}

@Service
class KafkaPipelineService(
    private val kafkaConsumerService: DynamicKafkaConsumerService,
    private val pipelineRepository: PipelineRepository
) {

    /** ✅ 파이프라인 등록 */
    fun registerPipeline(groupId: String, topics: List<String>): List<String>? {
        if (topics.isEmpty()) {
            logger.warn { "❌ 등록할 토픽이 없습니다. (groupId: $groupId)" }
            return null
        }

        // DB에 저장
        val pipeline = PipelineEntity.from(groupId, topics)
        pipelineRepository.save(pipeline)

        topics.forEachIndexed { index, topic ->
            kafkaConsumerService.startListening(ConsumerKey(groupId, topic)) { receive ->
                logger.info { "📨 [$topic] 받은 메시지: $receive" }

                // 다음 토픽으로 메시지 전달
                val nextTopic = topics.getOrNull(index + 1)
                if (nextTopic != null) {
                    kafkaConsumerService.sendMessage(nextTopic, receive)
                    logger.info { "➡️ 메시지 [$receive] 를 [$topic] → [$nextTopic] 로 전달" }
                } else {
                    logger.info { "✅ 최종 토픽 [$topic] 에 도착: $receive" }
                }
            }
        }

        waitForConsumersActivation(groupId, topics)

        return topics
    }

    /** ✅ 모든 토픽의 `activeConsumers`가 `true`가 될 때까지 대기 */
    private fun waitForConsumersActivation(groupId: String, topics: List<String>): Boolean {
        val maxWaitTimeMillis = 15000L  // 최대 대기 시간 (15초)
        val startTime = System.currentTimeMillis()

        while (System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
            if (topics.all { kafkaConsumerService.getConsumerStatus()[ConsumerKey(groupId, it)] == true }) {
                logger.info { "✅ 파이프라인 등록 완료 (groupId: $groupId)" }
                return true
            }
            Thread.sleep(500)  // 0.5초마다 상태 체크
        }

        logger.warn { "⚠️ 파이프라인 등록이 지연됨 (groupId: $groupId) - 일부 토픽이 활성화되지 않음" }
        return false
    }


    fun runPipeline(groupId: String, message: String) {
        val pipeline = pipelineRepository.findById(groupId).orElse(null)
        if (pipeline == null) {
            logger.warn { "❌ 파이프라인 [$groupId] 없음" }
            return
        }

        val topics = pipeline.getTopics()

        logger.info { "🚀 파이프라인 [$groupId] 실행 시작: ${topics.joinToString(" -> ")}" }

        val firstTopic = topics.first()
        kafkaConsumerService.sendMessage(firstTopic, message) // ✅ 첫 번째 토픽으로 메시지 전송
    }

    /** ✅ 모든 파이프라인 조회 */
    fun getAllPipelines(): Map<String, List<String>> {
        return pipelineRepository.findAll()
            .associate { it.groupId to it.getTopics() }
    }

    /** ✅ 특정 파이프라인 삭제 */
    fun deletePipeline(groupId: String) {
        if (pipelineRepository.existsById(groupId)) {
            pipelineRepository.deleteById(groupId)
            logger.info { "🗑️ 파이프라인 삭제 완료 (groupId: $groupId)" }
        } else {
            logger.warn { "⚠️ 삭제할 파이프라인 없음 (groupId: $groupId)" }
        }
    }
}