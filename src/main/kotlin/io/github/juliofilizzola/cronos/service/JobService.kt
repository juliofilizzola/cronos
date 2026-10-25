package io.github.juliofilizzola.cronos.service

import io.github.juliofilizzola.cronos.api.CreateJobRequest
import io.github.juliofilizzola.cronos.domain.*
import io.github.juliofilizzola.cronos.persistence.JobRepository
import io.github.juliofilizzola.cronos.scheduling.NextFireCalculator
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.DateTimeException
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.UUID

@Service
class JobService(
    private val repository: JobRepository,
    private val calculator: NextFireCalculator,
    private val clock: Clock,
) {

    fun create(request: CreateJobRequest): JobDetails {
        val zone = try {
            ZoneId.of(request.timezone)
        } catch (e: DateTimeException) {
            throw InvalidScheduleException("Fuso horário inválido: ${request.timezone}")
        }
        val now = clock.instant()

        val job = JobDetails(
            id = UUID.randomUUID(),
            name = request.name,
            scheduleType = request.scheduleType,
            scheduleExpr = request.scheduleExpr,
            timezone = zone.id,
            payload = request.payload,
            status = JobStatus.ACTIVE,
            nextFireAt = firstFireAt(request, zone, now),
            maxAttempts = request.maxAttempts,
            timeoutSeconds = request.timeoutSeconds,
            misfirePolicy = request.misfirePolicy,
            misfireThresholdSeconds = request.misfireThresholdSeconds,
            allowOverlap = request.allowOverlap,
            createdAt = now,
        )
        repository.insert(job)
        return job
    }

    fun get(id: UUID): JobDetails =
        repository.findById(id) ?: throw NotFoundException("Job $id não encontrado")

    fun list(status: JobStatus?, page: Int, size: Int): List<JobDetails> =
        repository.findAll(status, size, page * size)

    fun executions(jobId: UUID, page: Int, size: Int): List<ExecutionView> {
        get(jobId) // garante 404 se o job não existe
        return repository.findExecutions(jobId, size, page * size)
    }

    fun pause(id: UUID): JobDetails = transition(id, JobStatus.ACTIVE, JobStatus.PAUSED)

    /**
     * Retomar não recalcula next_fire_at de propósito: se o horário já passou durante a pausa,
     * a misfire policy do job decide o que fazer com o que foi perdido.
     */
    fun resume(id: UUID): JobDetails = transition(id, JobStatus.PAUSED, JobStatus.ACTIVE)

    private fun transition(id: UUID, from: JobStatus, to: JobStatus): JobDetails {
        if (repository.updateStatus(id, from, to) == 0) {
            val current = get(id) // lança 404 se não existir
            throw ConflictException("Job está ${current.status}, esperado $from para ir a $to")
        }
        return get(id)
    }

    private fun firstFireAt(request: CreateJobRequest, zone: ZoneId, now: Instant): Instant {
        if (request.scheduleType == ScheduleType.ONCE) {
            val at = try {
                Instant.parse(request.scheduleExpr)
            } catch (e: DateTimeException) {
                throw InvalidScheduleException("Para ONCE use um instante ISO-8601, ex.: 2026-10-30T12:00:00Z")
            }
            if (!at.isAfter(now)) throw InvalidScheduleException("O instante de um job ONCE deve estar no futuro")
            return at
        }

        val probe = Job(
            id = UUID.randomUUID(),
            name = request.name,
            scheduleType = request.scheduleType,
            scheduleExpr = request.scheduleExpr,
            timezone = zone,
            status = JobStatus.ACTIVE,
            nextFireAt = null,
            misfirePolicy = request.misfirePolicy,
            misfireThreshold = Duration.ofSeconds(request.misfireThresholdSeconds.toLong()),
            allowOverlap = request.allowOverlap,
        )

        val next = try {
            if (request.scheduleType == ScheduleType.INTERVAL &&
                Duration.parse(request.scheduleExpr) < Duration.ofSeconds(1)
            ) {
                throw InvalidScheduleException("O intervalo mínimo é 1 segundo (PT1S)")
            }
            calculator.nextAfter(probe, now)
        } catch (e: InvalidScheduleException) {
            throw e
        } catch (e: IllegalArgumentException) {
            throw InvalidScheduleException("Expressão inválida (${request.scheduleType}): ${e.message}")
        } catch (e: DateTimeException) {
            throw InvalidScheduleException("Expressão inválida (${request.scheduleType}): ${e.message}")
        }
        return next ?: throw InvalidScheduleException("A expressão nunca dispara")
    }
}