package net.musma.dandi.dynamictopic.kafka

import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.PostConstruct
import net.musma.dandi.dynamictopic.domain.PipelineNode
import net.musma.dandi.dynamictopic.domain.PipelineRepository
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.MessageListener
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

@Service
class DynamicKafkaConsumerService(
    private val kafkaProperties: KafkaProperties,
    private val pipelineRepository: PipelineRepository
) {
    private val containers = ConcurrentHashMap<ConsumerKey, ConcurrentMessageListenerContainer<String, String>>()
    private val activeConsumers = ConcurrentHashMap<ConsumerKey, Boolean>() // ✅ 구독 상태 추적

    @PostConstruct
    fun initializeConsumers() {
        logger.info { "🔄 서버 시작 시 Kafka 컨슈머 초기화 중..." }

        val allPipelines = pipelineRepository.findAll()
        allPipelines.forEach { pipeline ->
            val groupId = pipeline.id
            val rootNode = pipeline.rootNode ?: return@forEach
            logger.info { "🔄 파이프라인 등록: $groupId" }
            registerConsumersRecursively(groupId, rootNode)
            waitForConsumersActivation(groupId, rootNode)
            logger.info { "🔄 파이프라인 등록 완료: $groupId" }
        }

        logger.info { "✅ 모든 Kafka 컨슈머 자동 등록 완료" }
    }

    /**
     * ✅ 토픽 구독 시작
     */
    fun startListening(consumerKey: ConsumerKey, onMessage: (String) -> Unit) {
        val (groupId, topic) = consumerKey
        if (containers.containsKey(consumerKey)) {
            logger.info { "🟢 기존 컨슈머 사용: $consumerKey" }
            return
        }

        val containerProperties = createContainerProperties(consumerKey, onMessage)
        val consumerFactory = DefaultKafkaConsumerFactory<String, String>(createConsumerProperties())
        val container = ConcurrentMessageListenerContainer(consumerFactory, containerProperties)

        container.start()
        containers[consumerKey] = container
        activeConsumers[consumerKey] = false // 초기에 false 설정
        logger.info { "🔄 Kafka 컨슈머가 [$topic] 구독 중... (groupId: $groupId)" }
    }

    /**
     * ✅ 토픽 구독 중지
     */
    fun stopListening(topic: String, groupId: String) {
        val consumerKey = ConsumerKey(groupId, topic)
        try {
            val container = containers[consumerKey]
            if (container != null) {
                container.stop()
                if (!container.isRunning) { // ✅ 컨슈머가 완전히 중지되었는지 확인 후 제거
                    containers.remove(consumerKey)
                    activeConsumers.remove(consumerKey)
                    logger.info { "🛑 토픽 [$topic] 구독 중지 완료 (groupId: $groupId)" }
                } else {
                    logger.warn { "⚠️ 컨슈머 중지 확인 필요 [$topic] (groupId: $groupId)" }
                }
            } else {
                logger.warn { "⚠️ 구독 중이 아닌 토픽 [$topic] (groupId: $groupId)" }
            }
        } catch (e: Exception) {
            logger.error(e) { "❌ 토픽 [$topic] 구독 중지 실패 (groupId: $groupId)" }
        }
    }

    /**
     * ✅ 메시지 리스너 설정
     */
    private fun createContainerProperties(
        consumerKey: ConsumerKey,
        onMessage: (String) -> Unit
    ): ContainerProperties {
        val topic = consumerKey.toTopic()
        return ContainerProperties(topic).apply {
            setConsumerRebalanceListener(
                object : ConsumerRebalanceListener {
                    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
                        logger.info { "✅ Kafka 파티션 할당 완료! Topic: ${topic}, Assigned Partitions: $partitions" }
                        activeConsumers[consumerKey] = true
                    }

                    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
                        logger.warn { "⚠️ Kafka 파티션 회수됨! Topic: ${topic}, Revoked Partitions: $partitions" }
                        activeConsumers[consumerKey] = false
                    }
                },
            )

            /**
             * ✅ 메시지 처리 로직
             */
            messageListener = MessageListener { record ->
                handleMessage(record, consumerKey, onMessage)
            }
        }
    }

    /**
     * ✅ 메시지 처리 (전처리 후 전달)
     */
    private fun handleMessage(record: ConsumerRecord<String, String>, consumerKey: ConsumerKey, onMessage: (String) -> Unit) {
        val maxWaitTimeMillis = 5000L  // 최대 대기 시간 (5초)
        val intervalMillis = 100L       // 체크 간격 (100ms)
        var waitedTime = 0L

        // ✅ activeConsumers[consumerKey] 가 true가 될 때까지 대기
        while (activeConsumers[consumerKey] != true) {
            logger.warn { "⏳ 메시지 처리 대기 중: [$consumerKey] 활성화 대기 중... -> ${record.value()}" }
            if (waitedTime >= maxWaitTimeMillis) {
                logger.warn { "⚠️ 메시지 처리 지연: [$consumerKey] 활성화 대기 초과 (메시지 폐기 가능성 있음) -> ${record.value()}" }
                return // 메시지 폐기 (or 대기 초과 시 처리 방법 결정 가능)
            }

            Thread.sleep(intervalMillis)  // 100ms 대기 후 다시 확인
            waitedTime += intervalMillis
        }

        // ✅ 정상적으로 활성화 후 메시지 처리
        try {
            val processedMessage = processMessage(record)
            onMessage(processedMessage) // ✅ 메시지 전달 후 콜백 실행
        } catch (e: Exception) {
            logger.error(e) { "❌ 메시지 처리 실패: ${record.value()}" }
        }
    }

    /**
     * ✅ 메시지 전처리
     */
    private fun processMessage(record: ConsumerRecord<String, String>): String {
        logger.info { "📩 [${record.topic()}] 메시지 수신: ${record.value()}" }
        return record.value()
    }

    /**
     * ✅ 메시지 발행
     */
    fun sendMessage(consumerKey: ConsumerKey, message: String) {
        KafkaProducer<String, String>(createProducerProperties()).use { producer ->
            producer.send(ProducerRecord(consumerKey.toTopic(), message))
            logger.info { "📤 메시지 전송 완료: [$message] → [${consumerKey.toTopic()}]" }
        }
    }

    /**
     * ✅ Kafka 컨슈머 설정
     */
    private fun createConsumerProperties() = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaProperties.bootstrapServers,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to kafkaProperties.consumer.autoOffsetReset,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to kafkaProperties.consumer.enableAutoCommit,
        ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG to kafkaProperties.properties.request.timeoutMs,
        ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG to kafkaProperties.properties.connections.maxIdleMs,
        ConsumerConfig.GROUP_ID_CONFIG to kafkaProperties.consumer.groupId,
        ConsumerConfig.METADATA_MAX_AGE_CONFIG to "10000",
        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG to "10000",
    )

    /**
     * ✅ Kafka 프로듀서 설정
     */
    private fun createProducerProperties(): Properties {
        return Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        }
    }

    /**
     * ✅ 현재 활성화된 구독 상태 조회
     */
    fun getConsumerStatus(): Map<ConsumerKey, Boolean> = activeConsumers.toMap()

    /**
     * ✅ 기존 컨슈머 가져오기
     */
    fun getContainer(consumerKey: ConsumerKey): ConcurrentMessageListenerContainer<String, String>? {
        return containers[consumerKey]
    }

    fun registerConsumersRecursively(groupId: String, node: PipelineNode) {
        startListening(ConsumerKey(groupId, node.topic)) { receive ->
            logger.info { "📨 [${node.topic}] 받은 메시지: $receive" }

            // ✅ 자식 노드로 메시지 전달
            node.children.forEach { child ->
                sendMessage(ConsumerKey(groupId, child.topic), receive)
                logger.info { "➡️ 메시지 [$receive] 를 [${node.topic}] → [${child.topic}] 로 전달" }
            }
        }

        // ✅ 재귀적으로 모든 자식 노드에 대해 Kafka Consumer 등록
        node.children.forEach { child -> registerConsumersRecursively(groupId, child) }
    }

    /** ✅ 모든 토픽의 `activeConsumers`가 `true`가 될 때까지 대기 */
    fun waitForConsumersActivation(groupId: String, node: PipelineNode): Boolean {
        val maxWaitTimeMillis = 15000L  // 최대 대기 시간 (15초)
        val startTime = System.currentTimeMillis()

        while (System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
            if (allConsumersActivated(groupId, node)) {
                logger.info { "✅ 파이프라인 등록 완료 (groupId: $groupId)" }
                return true
            }
            Thread.sleep(500)  // 0.5초마다 상태 체크
        }

        logger.warn { "⚠️ 파이프라인 등록이 지연됨 (groupId: $groupId) - 일부 토픽이 활성화되지 않음" }
        return false
    }

    /** ✅ 모든 토픽이 활성화되었는지 확인하는 함수 */
    private fun allConsumersActivated(groupId: String, node: PipelineNode): Boolean {
        val isActive = getConsumerStatus()[ConsumerKey(groupId, node.topic)] == true

        // 모든 자식 노드도 활성화 상태인지 확인 (재귀 호출)
        return isActive && node.children.all { allConsumersActivated(groupId, it) }
    }
}