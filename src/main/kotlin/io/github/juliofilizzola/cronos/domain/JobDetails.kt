package io.github.juliofilizzola.cronos.domain

import tools.jackson.databind.JsonNode
import java.time.Instant
import java.util.UUID

/** Visão completa de um job, como é guardada e devolvida pela API. */
data class JobDetails(
    val id: UUID,
    val name: String,
    val scheduleType: ScheduleType,
    val scheduleExpr: String,
    val timezone: String,
    val payload: JsonNode,
    val status: JobStatus,
    val nextFireAt: Instant?,
    val maxAttempts: Int,
    val timeoutSeconds: Int,
    val misfirePolicy: MisfirePolicy,
    val misfireThresholdSeconds: Int,
    val allowOverlap: Boolean,
    val createdAt: Instant,
)

data class ExecutionView(
    val id: UUID,
    val jobId: UUID,
    val scheduledFor: Instant,
    val status: String,
    val attempt: Int,
    val runAfter: Instant,
    val startedAt: Instant?,
    val finishedAt: Instant?,
    val lastError: String?,
    val triggeredBy: String,
)