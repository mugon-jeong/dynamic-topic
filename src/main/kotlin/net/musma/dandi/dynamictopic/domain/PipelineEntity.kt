package net.musma.dandi.dynamictopic.domain

import jakarta.persistence.CascadeType
import jakarta.persistence.Entity
import jakarta.persistence.FetchType
import jakarta.persistence.Id
import jakarta.persistence.JoinColumn
import jakarta.persistence.OneToOne
import jakarta.persistence.Table


@Entity
@Table(name = "pipeline")
data class PipelineEntity(
    @Id
    val groupId: String,

    @OneToOne(cascade = [CascadeType.ALL], orphanRemoval = true, fetch = FetchType.EAGER)
    @JoinColumn(name = "root_node_id")
    var rootNode: PipelineNode? = null  // ✅ 루트 노드는 하나만 존재
)