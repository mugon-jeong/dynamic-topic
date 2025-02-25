package net.musma.dandi.dynamictopic.domain

import org.springframework.data.jpa.repository.JpaRepository

interface PipelineNodeRepository :JpaRepository<PipelineNode, Long> {
}