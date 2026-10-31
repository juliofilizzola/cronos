package io.github.juliofilizzola.cronos.scheduling

import io.github.juliofilizzola.cronos.domain.*
import io.github.juliofilizzola.cronos.persistence.ExecutionRepository
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
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
    private val executions: ExecutionRepository,
    private val tx: TransactionTemplate,
    private val resolver: MisfireResolver,
    private val clock: Clock,
    private val meters: MeterRegistry,
    @Value("\${cronos.scheduler.batch-size:100}") private val batchSize: Int,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private val created = Counter.builder("cronos.scheduler.executions.created")
        .description("Execuções criadas pelo scheduler").register(meters)
    private val duplicates = Counter.builder("cronos.scheduler.executions.duplicate")
        .description("Ocorrências que outra instância já tinha criado (ON CONFLICT)").register(meters)
    private val overlapSkipped = Counter.builder("cronos.scheduler.overlap.skipped")
        .description("Ocorrências ignoradas porque o job não permite sobreposição").register(meters)
    private val invalid = Counter.builder("cronos.scheduler.jobs.invalid")
        .description("Jobs pausados por agendamento inválido").register(meters)
    private val lag = Timer.builder("cronos.scheduler.lag")
        .description("Atraso entre o next_fire_at do job e o momento em que o scheduler o processou")
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(meters)

    @Scheduled(fixedDelayString = "\${cronos.scheduler.poll-interval-ms:1000}")
    fun tick() {
        // Se o lote veio cheio, ainda pode haver jobs vencidos: continua sem esperar.
        do {
            val processed = tx.execute { processBatch() } ?: 0
        } while (processed == batchSize)
    }

    /**
     * Processa um lote numa transação. As escritas vão em lote (batchUpdate): o custo por
     * job fica em memória, e o banco recebe poucas idas por lote em vez de duas por job.
     */
    private fun processBatch(): Int {
        val now = clock.instant()
        val jobs = jdbc.query(DUE_JOBS_SQL, JOB_MAPPER, utc(now), batchSize)
        if (jobs.isEmpty()) return 0

        // allow_overlap = false: não cria nova execução se já existe uma ativa.
        val busy = executions.withActive(jobs.filterNot { it.allowOverlap }.map { it.id })

        val toInsert = mutableListOf<Pair<UUID, Instant>>()
        val toUpdate = mutableListOf<Pair<Job, Resolution>>()
        for (job in jobs) {
            val resolution = resolve(job, now) ?: continue
            var fireAt = resolution.fireAt
            if (!job.allowOverlap) {
                // Uma execução recém-criada (PENDING) também conta como ativa: no máximo uma por job.
                val allowed = if (job.id in busy) 0 else 1
                fireAt.drop(allowed).forEach {
                    log.info("Job {} sem sobreposição: ocorrência {} ignorada", job.id, it)
                    overlapSkipped.increment()
                }
                fireAt = fireAt.take(allowed)
            }
            fireAt.forEach { toInsert += job.id to it }
            toUpdate += job to resolution
        }

        val inserted = executions.insertAll(toInsert, now, "SCHEDULER")
        created.increment(inserted.toDouble())
        duplicates.increment((toInsert.size - inserted).toDouble())

        jdbc.batchUpdate(
            "UPDATE jobs SET next_fire_at = ?, status = ? WHERE id = ?",
            toUpdate.map { (job, r) ->
                val status = if (r.nextFireAt == null) JobStatus.COMPLETED else JobStatus.ACTIVE
                arrayOf<Any?>(r.nextFireAt?.let(::utc), status.name, job.id)
            },
        )
        return jobs.size
    }

    /** Decide o que disparar. Retorna null (e pausa o job) se o agendamento for inválido. */
    private fun resolve(job: Job, now: Instant): Resolution? {
        val resolution = try {
            resolver.resolve(job, now)
        } catch (e: Exception) {
            // Cron/intervalo inválido: pausa o job em vez de travar o lote inteiro.
            log.error("Job {} ({}) com agendamento inválido, pausando", job.id, job.name, e)
            jdbc.update("UPDATE jobs SET status = ?, next_fire_at = NULL WHERE id = ?",
                JobStatus.PAUSED.name, job.id)
            invalid.increment()
            return null
        }

        job.nextFireAt?.let { lag.record(Duration.between(it, now).coerceAtLeast(Duration.ZERO)) }

        if (resolution.misfired) {
            meters.counter("cronos.scheduler.misfires", "policy", job.misfirePolicy.name).increment()
            log.warn("Misfire no job {} ({}): policy={}, atraso={}s, execuções criadas={}",
                job.id, job.name, job.misfirePolicy,
                Duration.between(job.nextFireAt, now).seconds, resolution.fireAt.size)
        }
        return resolution
    }

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