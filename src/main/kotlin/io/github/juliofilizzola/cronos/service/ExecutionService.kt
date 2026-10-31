package io.github.juliofilizzola.cronos.service

import io.github.juliofilizzola.cronos.domain.ConflictException
import io.github.juliofilizzola.cronos.domain.ExecutionView
import io.github.juliofilizzola.cronos.domain.NotFoundException
import io.github.juliofilizzola.cronos.persistence.ExecutionRepository
import org.springframework.stereotype.Service
import java.time.Clock
import java.util.UUID

@Service
class ExecutionService(
    private val repository: ExecutionRepository,
    private val jobs: JobService,
    private val clock: Clock,
) {

    fun get(id: UUID): ExecutionView =
        repository.findById(id) ?: throw NotFoundException("Execução $id não encontrada")

    /**
     * Disparo manual: cria uma execução agora, fora do agendamento. Vale para jobs em
     * qualquer status (inclusive PAUSED e COMPLETED) e não altera o next_fire_at.
     * Respeita allowOverlap: sem sobreposição, recusa se já houver uma execução ativa.
     */
    fun trigger(jobId: UUID): ExecutionView {
        val job = jobs.get(jobId)
        if (!job.allowOverlap && repository.hasActive(jobId)) {
            throw ConflictException("Job $jobId não permite sobreposição e já tem uma execução ativa")
        }
        val now = clock.instant()
        val id = UUID.randomUUID()
        if (repository.insert(id, jobId, now, now, triggeredBy = "MANUAL") == 0) {
            throw ConflictException("Já existe uma execução do job $jobId para $now")
        }
        return get(id)
    }

    /** Cancela uma execução PENDING ou RUNNING. Uma RUNNING é interrompida pelo worker. */
    fun cancel(id: UUID): ExecutionView {
        if (repository.cancel(id, clock.instant()) == 0) {
            val current = get(id)
            throw ConflictException("Execução está ${current.status}, só PENDING ou RUNNING podem ser canceladas")
        }
        return get(id)
    }

    /** Recoloca na fila uma execução DEAD ou FAILED, com as tentativas zeradas. */
    fun retry(id: UUID): ExecutionView {
        if (repository.requeue(id, clock.instant()) == 0) {
            val current = get(id)
            throw ConflictException("Execução está ${current.status}, só DEAD ou FAILED podem ser retentadas")
        }
        return get(id)
    }
}
