package io.github.juliofilizzola.cronos.persistence

import io.github.juliofilizzola.cronos.domain.*
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.stereotype.Repository
import tools.jackson.databind.json.JsonMapper
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

@Repository
class JobRepository(
    private val jdbc: JdbcTemplate,
    private val jsonMapper: JsonMapper,
) {

    fun insert(job: JobDetails) {
        jdbc.update(
            """
            INSERT INTO jobs (id, name, schedule_type, schedule_expr, timezone, payload, status,
                              next_fire_at, max_attempts, timeout_seconds, misfire_policy,
                              misfire_threshold_seconds, allow_overlap, created_at)
            VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            job.id, job.name, job.scheduleType.name, job.scheduleExpr, job.timezone,
            jsonMapper.writeValueAsString(job.payload), job.status.name,
            job.nextFireAt?.let(::utc), job.maxAttempts, job.timeoutSeconds, job.misfirePolicy.name,
            job.misfireThresholdSeconds, job.allowOverlap, utc(job.createdAt),
        )
    }

    fun findById(id: UUID): JobDetails? =
        jdbc.query("SELECT $JOB_COLUMNS FROM jobs WHERE id = ?", jobMapper, id).firstOrNull()

    fun findAll(status: JobStatus?, limit: Int, offset: Int): List<JobDetails> =
        if (status == null) {
            jdbc.query(
                "SELECT $JOB_COLUMNS FROM jobs ORDER BY created_at DESC LIMIT ? OFFSET ?",
                jobMapper, limit, offset,
            )
        } else {
            jdbc.query(
                "SELECT $JOB_COLUMNS FROM jobs WHERE status = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
                jobMapper, status.name, limit, offset,
            )
        }

    /** Só muda se o status atual for [from]. Retorna quantas linhas mudaram (0 ou 1). */
    fun updateStatus(id: UUID, from: JobStatus, to: JobStatus): Int =
        jdbc.update("UPDATE jobs SET status = ? WHERE id = ? AND status = ?", to.name, id, from.name)

    fun findExecutions(jobId: UUID, limit: Int, offset: Int): List<ExecutionView> =
        jdbc.query(
            """
            SELECT id, job_id, scheduled_for, status, attempt, run_after, started_at, finished_at, last_error
            FROM executions WHERE job_id = ?
            ORDER BY scheduled_for DESC LIMIT ? OFFSET ?
            """.trimIndent(),
            executionMapper, jobId, limit, offset,
        )

    private val jobMapper = RowMapper { rs, _ ->
        JobDetails(
            id = rs.getObject("id", UUID::class.java),
            name = rs.getString("name"),
            scheduleType = ScheduleType.valueOf(rs.getString("schedule_type")),
            scheduleExpr = rs.getString("schedule_expr"),
            timezone = rs.getString("timezone"),
            payload = jsonMapper.readTree(rs.getString("payload")),
            status = JobStatus.valueOf(rs.getString("status")),
            nextFireAt = instant(rs.getObject("next_fire_at", OffsetDateTime::class.java)),
            maxAttempts = rs.getInt("max_attempts"),
            timeoutSeconds = rs.getInt("timeout_seconds"),
            misfirePolicy = MisfirePolicy.valueOf(rs.getString("misfire_policy")),
            misfireThresholdSeconds = rs.getInt("misfire_threshold_seconds"),
            allowOverlap = rs.getBoolean("allow_overlap"),
            createdAt = instant(rs.getObject("created_at", OffsetDateTime::class.java))!!,
        )
    }

    private val executionMapper = RowMapper { rs, _ ->
        ExecutionView(
            id = rs.getObject("id", UUID::class.java),
            jobId = rs.getObject("job_id", UUID::class.java),
            scheduledFor = instant(rs.getObject("scheduled_for", OffsetDateTime::class.java))!!,
            status = rs.getString("status"),
            attempt = rs.getInt("attempt"),
            runAfter = instant(rs.getObject("run_after", OffsetDateTime::class.java))!!,
            startedAt = instant(rs.getObject("started_at", OffsetDateTime::class.java)),
            finishedAt = instant(rs.getObject("finished_at", OffsetDateTime::class.java)),
            lastError = rs.getString("last_error"),
        )
    }

    private fun utc(i: Instant): OffsetDateTime = OffsetDateTime.ofInstant(i, ZoneOffset.UTC)
    private fun instant(o: OffsetDateTime?): Instant? = o?.toInstant()

    private companion object {
        const val JOB_COLUMNS = "id, name, schedule_type, schedule_expr, timezone, payload, status, " +
                "next_fire_at, max_attempts, timeout_seconds, misfire_policy, " +
                "misfire_threshold_seconds, allow_overlap, created_at"
    }
}