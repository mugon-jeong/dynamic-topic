package net.musma.dandi.dynamictopic.domain

import jakarta.persistence.*

@Entity
@Table(name = "pipeline_node")
data class PipelineNode(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(nullable = false)
    val topic: String,

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "parent_id")
    var parent: PipelineNode? = null,

    @OneToMany(mappedBy = "parent", cascade = [CascadeType.ALL], orphanRemoval = true, fetch = FetchType.EAGER)
    val children: MutableList<PipelineNode> = mutableListOf()
)