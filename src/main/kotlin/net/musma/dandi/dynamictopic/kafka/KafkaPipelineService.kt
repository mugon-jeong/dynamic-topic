package net.musma.dandi.dynamictopic.kafka

import io.github.oshai.kotlinlogging.KotlinLogging
import net.musma.dandi.dynamictopic.domain.PipelineEntity
import net.musma.dandi.dynamictopic.domain.PipelineNode
import net.musma.dandi.dynamictopic.domain.PipelineRepository
import org.springframework.stereotype.Service

private val logger = KotlinLogging.logger {}

@Service
class KafkaPipelineService(
    private val kafkaConsumerService: DynamicKafkaConsumerService,
    private val pipelineRepository: PipelineRepository
) {

    /** ✅ 파이프라인 등록 */
    fun registerPipeline(groupId: String, rootTopic: String, topicRelations: Map<String, List<String>>): PipelineEntity {
        // ✅ 기존 파이프라인 삭제 후 새로 등록
        pipelineRepository.deleteById(groupId)
        pipelineRepository.flush()

        // ✅ 새로운 루트 노드 생성
        val rootNode = PipelineNode(topic = rootTopic)
        val nodeMap = mutableMapOf(rootTopic to rootNode)

        // ✅ topicRelations의 부모 키들을 루트 노드의 자식으로 추가
        topicRelations.keys.forEach { parentTopic ->
            val parentNode = nodeMap.getOrPut(parentTopic) { PipelineNode(topic = parentTopic) }
            if (parentNode.parent == null) {  // ✅ 중복 방지
                parentNode.parent = rootNode
                rootNode.children.add(parentNode)
            }
        }

        // ✅ topicRelations를 기반으로 부모-자식 관계 구성
        topicRelations.forEach { (parentTopic, childTopics) ->
            val parentNode = nodeMap.getOrPut(parentTopic) { PipelineNode(topic = parentTopic) }
            childTopics.forEach { childTopic ->
                val childNode = nodeMap.getOrPut(childTopic) { PipelineNode(topic = childTopic) }
                if (childNode.parent == null) {  // ✅ 중복 방지
                    childNode.parent = parentNode
                    parentNode.children.add(childNode)
                }
            }
        }

        // ✅ DB에 저장
        val pipeline = PipelineEntity(groupId, rootNode)
        val pipelineEntity = pipelineRepository.saveAndFlush(pipeline)

        // ✅ 트리 구조를 기반으로 Kafka Consumer 등록
        kafkaConsumerService.registerConsumersRecursively(groupId, rootNode)

        // ✅ 모든 토픽이 활성화될 때까지 대기
        kafkaConsumerService.waitForConsumersActivation(groupId, rootNode)

        return pipelineEntity
    }


    fun runPipeline(groupId: String, message: String) {
        val pipeline = pipelineRepository.findById(groupId).orElse(null)
        if (pipeline == null) {
            logger.warn { "❌ 파이프라인 [$groupId] 없음" }
            return
        }

        val rootNode = pipeline.rootNode ?: return

        logger.info { "🚀 파이프라인 [$groupId] 실행 시작:" }
        printPipelineTree(rootNode, 0) // ✅ 트리 구조 로그 출력

        kafkaConsumerService.sendMessage(rootNode.topic, message) // ✅ 첫 번째 토픽으로 메시지 전송
    }

    /** ✅ 트리 구조를 로그로 출력하는 함수 */
    private fun printPipelineTree(node: PipelineNode, depth: Int) {
        val indent = "  ".repeat(depth) // ✅ 들여쓰기 (depth만큼 공백 추가)
        logger.info { "$indent- ${node.topic}" }

        node.children.forEach { child ->
            printPipelineTree(child, depth + 1) // ✅ 재귀 호출로 트리 출력
        }
    }

    /** ✅ 모든 파이프라인 조회 */
    fun getAllPipelines(): MutableList<PipelineEntity> {
        return pipelineRepository.findAll()
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