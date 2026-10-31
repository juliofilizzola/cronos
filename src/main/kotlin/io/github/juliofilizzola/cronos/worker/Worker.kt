package io.github.juliofilizzola.cronos.worker

import io.github.juliofilizzola.cronos.persistence.ClaimedExecution
import io.github.juliofilizzola.cronos.persistence.ExecutionRepository
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.time.Clock
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

/**
 * Worker: reserva execuções PENDING, chama o webhook e grava o resultado.
 *
 * Seguro com várias instâncias rodando:
 *  1. O claim usa FOR UPDATE SKIP LOCKED: cada execução vai para um único worker.
 *  2. A reserva tem lease. O heartbeat renova enquanto a chamada está em andamento;
 *     se o worker morrer, o lease vence e o reaper devolve a execução para a fila.
 *  3. O resultado só é gravado se a execução ainda for deste worker (locked_by).
 *
 * A entrega é at-least-once: se o worker morrer depois de chamar o webhook e antes de
 * gravar o resultado, a execução roda de novo. O destino deve usar o Idempotency-Key.
 */
@Component
@ConditionalOnProperty(prefix = "cronos.worker", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class Worker(
    private val repository: ExecutionRepository,
    private val webhook: WebhookClient,
    private val retryPolicy: RetryPolicy,
    private val clock: Clock,
    private val props: WorkerProperties,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    val workerId: String = "${hostname()}-${ManagementFactory.getRuntimeMXBean().pid}-" +
            UUID.randomUUID().toString().take(8)

    private val slots = Semaphore(props.concurrency)
    private val executor = Executors.newVirtualThreadPerTaskExecutor()
    @Volatile private var stopping = false

    init {
        log.info("Worker {} iniciado (concorrência={})", workerId, props.concurrency)
    }

    @Scheduled(fixedDelayString = "\${cronos.worker.poll-interval-ms:500}")
    fun poll() {
        if (stopping) return
        // Só reserva o que consegue executar agora: o resto fica livre para outros workers.
        val free = slots.availablePermits()
        if (free == 0) return

        val now = clock.instant()
        val claimed = repository.claim(workerId, free, now, now.plus(props.lease))
        for (execution in claimed) {
            slots.acquire()
            executor.submit {
                try {
                    run(execution)
                } finally {
                    slots.release()
                }
            }
        }
    }

    @Scheduled(fixedDelayString = "\${cronos.worker.heartbeat-interval-ms:10000}")
    fun heartbeat() {
        if (slots.availablePermits() == props.concurrency) return // nada em andamento
        repository.extendLeases(workerId, clock.instant().plus(props.lease))
    }

    @Scheduled(fixedDelayString = "\${cronos.worker.reaper-interval-ms:5000}")
    fun reap() {
        val reaped = repository.reapExpired(clock.instant())
        if (reaped > 0) log.warn("Reaper recuperou {} execução(ões) com lease expirado", reaped)
    }

    private fun run(execution: ClaimedExecution) {
        val result = try {
            webhook.call(execution)
        } catch (e: Exception) {
            AttemptResult.Failure("erro inesperado: ${e.message}", retryable = true)
        }

        val now = clock.instant()
        val outcome = retryPolicy.decide(result, execution.attempt, execution.maxAttempts, now)
        val updated = try {
            when (outcome) {
                Outcome.Succeeded -> repository.finish(execution.id, workerId, "SUCCEEDED", now, null, null)
                is Outcome.Retry -> repository.finish(execution.id, workerId, "PENDING", null, outcome.runAfter, outcome.error)
                is Outcome.Dead -> repository.finish(execution.id, workerId, "DEAD", now, null, outcome.error)
                is Outcome.Failed -> repository.finish(execution.id, workerId, "FAILED", now, null, outcome.error)
            }
        } catch (e: Exception) {
            // O lease vai vencer e o reaper devolve a execução para a fila.
            log.error("Falha ao gravar resultado da execução {}", execution.id, e)
            return
        }

        if (updated == 0) {
            log.warn("Execução {} não pertence mais a {} (lease expirou?): resultado descartado",
                execution.id, workerId)
            return
        }
        when (outcome) {
            Outcome.Succeeded -> log.debug("Execução {} concluída (tentativa {})", execution.id, execution.attempt)
            is Outcome.Retry -> log.info("Execução {} falhou na tentativa {}/{}: {}. Nova tentativa em {}",
                execution.id, execution.attempt, execution.maxAttempts, outcome.error, outcome.runAfter)
            is Outcome.Dead -> log.warn("Execução {} DEAD após {} tentativas: {}",
                execution.id, execution.attempt, outcome.error)
            is Outcome.Failed -> log.warn("Execução {} FAILED sem retentativa: {}", execution.id, outcome.error)
        }
    }

    @PreDestroy
    fun shutdown() {
        stopping = true
        executor.shutdown()
        if (!executor.awaitTermination(props.shutdownGrace.toMillis(), TimeUnit.MILLISECONDS)) {
            log.warn("Execuções ainda em andamento após {}: o reaper vai recuperá-las", props.shutdownGrace)
            executor.shutdownNow()
        }
    }

    private fun hostname(): String = try {
        InetAddress.getLocalHost().hostName
    } catch (e: Exception) {
        "worker"
    }
}
