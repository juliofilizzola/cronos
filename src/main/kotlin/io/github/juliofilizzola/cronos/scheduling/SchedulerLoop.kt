package io.github.juliofilizzola.cronos.scheduling

import io.github.juliofilizzola.cronos.domain.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.UUID

/**
 * Loop do scheduler: transforma jobs vencidos em execuções PENDING.
 *
 * Seguro com várias instâncias rodando:
 *  1. FOR UPDATE SKIP LOCKED: cada instância pega um lote diferente de jobs.
 *  2. UNIQUE (job_id, scheduled_for) + ON CONFLICT DO NOTHING: última barreira
 *     contra execução duplicada.
 */
@Component
class SchedulerLoop(
    private val jdbc: JdbcTemplate,
    private val tx: TransactionTemplate,
    private val resolver: MisfireResolver,
    private val clock: Clock,
    @Value("\${cronos.scheduler.batch-size:100}") private val batchSize: Int,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${cronos.scheduler.poll-interval-ms:1000}")
    fun tick() {
        // Se o lote veio cheio, ainda pode haver jobs vencidos: continua sem esperar.
        do {
            val processed = tx.execute { processBatch() } ?: 0
        } while (processed == batchSize)
    }

    private fun processBatch(): Int {
        val now = clock.instant()
        val jobs = jdbc.query(DUE_JOBS_SQL, JOB_MAPPER, utc(now), batchSize)
        jobs.forEach { schedule(it, now) }
        return jobs.size
    }

    private fun schedule(job: Job, now: Instant) {
        val resolution = try {
            resolver.resolve(job, now)
        } catch (e: Exception) {
            // Cron/intervalo inválido: pausa o job em vez de travar o lote inteiro.
            log.error("Job {} ({}) com agendamento inválido, pausando", job.id, job.name, e)
            jdbc.update("UPDATE jobs SET status = ?, next_fire_at = NULL WHERE id = ?",
                JobStatus.PAUSED.name, job.id)
            return
        }

        if (resolution.misfired) {
            log.warn("Misfire no job {} ({}): policy={}, atraso={}s, execuções criadas={}",
                job.id, job.name, job.misfirePolicy,
                Duration.between(job.nextFireAt, now).seconds, resolution.fireAt.size)
        }

        for (scheduledFor in resolution.fireAt) {
            // allow_overlap = false: não cria nova execução se já existe uma ativa.
            // Uma execução recém-criada (PENDING) também conta como ativa.
            if (!job.allowOverlap && hasActiveExecution(job.id)) {
                log.info("Job {} sem sobreposição: ocorrência {} ignorada", job.id, scheduledFor)
                continue
            }
            insertExecution(job.id, scheduledFor, now)
        }

        val newStatus = if (resolution.nextFireAt == null) JobStatus.COMPLETED else JobStatus.ACTIVE
        jdbc.update(
            "UPDATE jobs SET next_fire_at = ?, status = ? WHERE id = ?",
            resolution.nextFireAt?.let(::utc), newStatus.name, job.id,
        )
    }

    private fun hasActiveExecution(jobId: UUID): Boolean =
        jdbc.queryForObject(
            "SELECT EXISTS (SELECT 1 FROM executions WHERE job_id = ? AND status IN ('PENDING','RUNNING'))",
            Boolean::class.java, jobId,
        ) == true

    /** Retorna 1 se criou, 0 se outra instância já tinha criado a mesma ocorrência. */
    private fun insertExecution(jobId: UUID, scheduledFor: Instant, now: Instant): Int =
        jdbc.update(
            """
            INSERT INTO executions (id, job_id, scheduled_for, status, run_after)
            VALUES (?, ?, ?, 'PENDING', ?)
            ON CONFLICT (job_id, scheduled_for) DO NOTHING
            """.trimIndent(),
            UUID.randomUUID(), jobId, utc(scheduledFor), utc(now),
        )

    private fun utc(i: Instant): OffsetDateTime = OffsetDateTime.ofInstant(i, ZoneOffset.UTC)

    private companion object {
        const val DUE_JOBS_SQL = """
            SELECT id, name, schedule_type, schedule_expr, timezone, status, next_fire_at,
                   misfire_policy, misfire_threshold_seconds, allow_overlap
            FROM jobs
            WHERE status = 'ACTIVE' AND next_fire_at <= ?
            ORDER BY next_fire_at
            LIMIT ?
            FOR UPDATE SKIP LOCKED
        """

        val JOB_MAPPER = RowMapper { rs, _ ->
            Job(
                id = rs.getObject("id", UUID::class.java),
                name = rs.getString("name"),
                scheduleType = ScheduleType.valueOf(rs.getString("schedule_type")),
                scheduleExpr = rs.getString("schedule_expr"),
                timezone = ZoneId.of(rs.getString("timezone")),
                status = JobStatus.valueOf(rs.getString("status")),
                nextFireAt = rs.getObject("next_fire_at", OffsetDateTime::class.java)?.toInstant(),
                misfirePolicy = MisfirePolicy.valueOf(rs.getString("misfire_policy")),
                misfireThreshold = Duration.ofSeconds(rs.getInt("misfire_threshold_seconds").toLong()),
                allowOverlap = rs.getBoolean("allow_overlap"),
            )
        }
    }
}