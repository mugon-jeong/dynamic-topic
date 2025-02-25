package net.musma.dandi.dynamictopic.domain

/**
 * ✅ 파이프라인 등록 요청 DTO
 */
data class PipelineDto(
    val id: String,      // 파이프라인 그룹 ID
    val name: String,    // 파이프라인 이름
    val nodes: List<NodeDto>, // 노드 리스트
    val edges: List<EdgeDto>  // 엣지 리스트
)

/**
 * ✅ 개별 노드 DTO
 */
data class NodeDto(
    val id: String,
    val type: String,
    val data: String
)


/**
 * ✅ 엣지(연결선) DTO
 */
data class EdgeDto(
    val source: String,
    val target: String
)