package net.musma.dandi.dynamictopic.domain

import com.fasterxml.jackson.annotation.JsonIgnore
import jakarta.persistence.*

@Entity
@Table(name = "pipeline_node")
data class PipelineNode(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(nullable = false)
    val topic: String,

    @Column(nullable = false)
    val type: String,

    @Column(nullable = false, columnDefinition = "TEXT")
    val data: String,

    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnore // ✅ 부모 노드 직렬화 방지
    @JoinColumn(name = "parent_id")
    var parent: PipelineNode? = null,

    @OneToMany(mappedBy = "parent", cascade = [CascadeType.ALL], orphanRemoval = true, fetch = FetchType.EAGER)
    val children: MutableList<PipelineNode> = mutableListOf()
)