package io.github.juliofilizzola.cronos.domain

import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.UUID

enum class ScheduleType { CRON, INTERVAL, ONCE }
enum class MisfirePolicy { FIRE_ALL, FIRE_ONCE, SKIP }
enum class JobStatus { ACTIVE, PAUSED, CANCELLED, COMPLETED }

data class Job(
    val id: UUID,
    val name: String,
    val scheduleType: ScheduleType,
    val scheduleExpr: String,          // cron (6 campos, com segundos), ISO-8601 Duration ou ignorado em ONCE
    val timezone: ZoneId,
    val status: JobStatus,
    val nextFireAt: Instant?,
    val misfirePolicy: MisfirePolicy,
    val misfireThreshold: Duration,
    val allowOverlap: Boolean,
)