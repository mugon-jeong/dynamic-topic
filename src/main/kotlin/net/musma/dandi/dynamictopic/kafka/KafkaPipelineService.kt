package net.musma.dandi.dynamictopic.kafka

import io.github.oshai.kotlinlogging.KotlinLogging
import net.musma.dandi.dynamictopic.domain.EdgeDto
import net.musma.dandi.dynamictopic.domain.NodeDto
import net.musma.dandi.dynamictopic.domain.PipelineDto
import net.musma.dandi.dynamictopic.domain.PipelineEntity
import net.musma.dandi.dynamictopic.domain.PipelineNode
import net.musma.dandi.dynamictopic.domain.PipelineRepository
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service

private val logger = KotlinLogging.logger {}

@Service
class KafkaPipelineService(
    private val kafkaConsumerService: DynamicKafkaConsumerService,
    private val pipelineRepository: PipelineRepository
) {
    /**
     * ✅ `edges`를 기반으로 부모-자식 관계 맵 생성
     */
    private fun convertToTopicRelations(edges: List<EdgeDto>, nodes: List<NodeDto>): Map<NodeDto, List<NodeDto>> {
        return edges.groupBy(
            { edge -> nodes.find { it.id == edge.source } ?: throw IllegalArgumentException("연결된 노드를 찾을 수 없음") },
            { edge -> nodes.find { it.id == edge.target } ?: throw IllegalArgumentException("연결된 노드를 찾을 수 없음") },
        )
    }

    /**
     * ✅ `Start` 노드를 찾아 루트 토픽 반환
     */
    private fun findRootTopic(nodes: List<NodeDto>): NodeDto? {
        return nodes.find { it.type == "Start" }
    }

    /** ✅ 파이프라인 등록 */
    fun registerPipeline(dto: PipelineDto): Map<String, String> {
        // ✅ 기존 파이프라인 삭제 후 새로 등록
        pipelineRepository.deleteById(dto.id)
        pipelineRepository.flush()

        // ✅ 루트 노드 찾기 (Start 노드가 rootTopic)
        val rootNode = findRootTopic(dto.nodes) ?: throw IllegalArgumentException("Start 노드가 필요합니다.")
        val rootTopic = rootNode.id
        // ✅ topicRelations 생성
        val topicRelations = convertToTopicRelations(dto.edges, dto.nodes)
        // ✅ 새로운 루트 노드 생성
        val root = PipelineNode(topic = rootNode.id, type = rootNode.type, data = rootNode.data)
        val nodeMap = mutableMapOf(rootTopic to root)

        // ✅ topicRelations 기반으로 트리 구성
        topicRelations.forEach { (parent, children) ->
            val parentNode = nodeMap.getOrPut(parent.id) { PipelineNode(topic = parent.id, type = parent.type, data = parent.data) }
            children.forEach { child ->
                val childNode = nodeMap.getOrPut(child.id) { PipelineNode(topic = child.id, type = child.type, data = child.data) }
                if (childNode.parent == null) {  // ✅ 중복 방지
                    childNode.parent = parentNode
                    parentNode.children.add(childNode)
                }
            }
        }

        // ✅ DB에 저장
        val pipeline = PipelineEntity(dto.id, dto.name, root)
        val pipelineEntity = pipelineRepository.saveAndFlush(pipeline)

        // ✅ 트리 구조를 기반으로 Kafka Consumer 등록
        kafkaConsumerService.registerConsumersRecursively(dto.id, root)

        // ✅ 모든 토픽이 활성화될 때까지 대기
        kafkaConsumerService.waitForConsumersActivation(dto.id, root)

        return mapOf("id" to pipelineEntity.id, "name" to pipelineEntity.name)
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

        kafkaConsumerService.sendMessage(ConsumerKey(pipeline.id, rootNode.topic), message) // ✅ 첫 번째 토픽으로 메시지 전송
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
    fun getAllPipelines(): List<Map<String, String>> {
        return pipelineRepository.findAll().map { mapOf("id" to it.id, "name" to it.name) }
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

    fun getPipeline(groupId: String): Map<String, Any>? {
        val pipeline =  pipelineRepository.findByIdOrNull(groupId) ?: return null
        val (nodes, edges) = pipeline.toNodesAndEdges()
        return mapOf(
            "id" to pipeline.id,
            "name" to pipeline.name,
            "nodes" to nodes,
            "edges" to edges
        )
    }
    /**
     * ✅ `PipelineEntity`를 `nodes`와 `edges`로 변환
     */
    fun PipelineEntity.toNodesAndEdges(): Pair<List<NodeDto>, List<EdgeDto>> {
        val nodes = mutableListOf<NodeDto>()
        val edges = mutableListOf<EdgeDto>()

        rootNode?.let { traverseTree(it, nodes, edges) }

        return nodes to edges
    }

    /**
     * ✅ 트리를 순회하며 `nodes`와 `edges` 생성
     */
    private fun traverseTree(node: PipelineNode, nodes: MutableList<NodeDto>, edges: MutableList<EdgeDto>) {
        // ✅ 현재 노드를 nodes 리스트에 추가
        nodes.add(
            NodeDto(
                id = node.topic,
                type = node.type,
                data = node.data
            )
        )

        // ✅ 자식 노드들에 대한 edges 리스트 생성
        node.children.forEach { child ->
            edges.add(EdgeDto(source = node.topic, target = child.topic))
            traverseTree(child, nodes, edges) // ✅ 재귀 호출로 트리 순회
        }
    }

}