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

    /**
     * Apaga o job e todo o histórico dele. Retorna false se o job não existe.
     *
     * Trava a linha do job antes de apagar as execuções: com o lock, o scheduler (SKIP LOCKED)
     * não consegue criar uma execução nova entre os dois DELETEs, o que quebraria a FK.
     * Deve rodar dentro de uma transação.
     */
    fun deleteWithHistory(id: UUID): Boolean {
        val locked = jdbc.queryForList("SELECT id FROM jobs WHERE id = ? FOR UPDATE", UUID::class.java, id)
        if (locked.isEmpty()) return false
        jdbc.update("DELETE FROM executions WHERE job_id = ?", id)
        jdbc.update("DELETE FROM jobs WHERE id = ?", id)
        return true
    }

    /** Só muda se o status atual for [from]. Retorna quantas linhas mudaram (0 ou 1). */
    fun updateStatus(id: UUID, from: JobStatus, to: JobStatus): Int =
        jdbc.update("UPDATE jobs SET status = ? WHERE id = ? AND status = ?", to.name, id, from.name)

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

    private fun utc(i: Instant): OffsetDateTime = OffsetDateTime.ofInstant(i, ZoneOffset.UTC)
    private fun instant(o: OffsetDateTime?): Instant? = o?.toInstant()

    private companion object {
        const val JOB_COLUMNS = "id, name, schedule_type, schedule_expr, timezone, payload, status, " +
                "next_fire_at, max_attempts, timeout_seconds, misfire_policy, " +
                "misfire_threshold_seconds, allow_overlap, created_at"
    }
}