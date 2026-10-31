package io.github.juliofilizzola.cronos.worker

import io.github.juliofilizzola.cronos.persistence.ClaimedExecution
import io.github.juliofilizzola.cronos.persistence.ExecutionRepository
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.time.Clock
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CompletableFuture
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
    private val meters: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    val workerId: String = "${hostname()}-${ManagementFactory.getRuntimeMXBean().pid}-" +
            UUID.randomUUID().toString().take(8)

    private val slots = Semaphore(props.concurrency)
    private val executor = Executors.newVirtualThreadPerTaskExecutor()
    /** Execuções em andamento nesta instância, para poder interrompê-las ao cancelar. */
    private val inFlight = ConcurrentHashMap<UUID, InFlight>()
    @Volatile private var stopping = false

    private val startDelay = Timer.builder("cronos.execution.start.delay")
        .description("Atraso entre o horário lógico (scheduled_for) e o início da 1ª tentativa")
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(meters)
    private val reaped = Counter.builder("cronos.worker.reaped")
        .description("Execuções recuperadas de workers que pararam de responder").register(meters)

    init {
        meters.gauge("cronos.worker.inflight", inFlight) { it.size.toDouble() }
        log.info("Worker {} iniciado (concorrência={})", workerId, props.concurrency)
    }

    @Scheduled(fixedDelayString = "\${cronos.worker.poll-interval-ms:500}")
    fun poll() {
        // Enquanto houver trabalho, continua reservando assim que um slot vagar,
        // sem esperar o próximo tick. Só reserva o que consegue executar agora:
        // o resto fica livre para outros workers.
        while (!stopping) {
            val free = slots.availablePermits()
            if (free == 0) {
                if (!slots.tryAcquire(props.pollIntervalMs, TimeUnit.MILLISECONDS)) return
                slots.release()
                continue
            }
            val now = clock.instant()
            val claimed = repository.claim(workerId, free, now, now.plus(props.lease))
            for (execution in claimed) {
                if (execution.attempt == 1) {
                    startDelay.record(Duration.between(execution.scheduledFor, now).coerceAtLeast(Duration.ZERO))
                }
                slots.acquire()
                val handle = InFlight()
                inFlight[execution.id] = handle
                executor.submit {
                    try {
                        run(execution, handle)
                    } finally {
                        inFlight.remove(execution.id)
                        slots.release()
                    }
                }
            }
            if (claimed.size < free) return // fila vazia por enquanto
        }
    }

    @Scheduled(fixedDelayString = "\${cronos.worker.heartbeat-interval-ms:10000}")
    fun heartbeat() {
        if (inFlight.isEmpty()) return
        repository.extendLeases(workerId, clock.instant().plus(props.lease))
    }

    /** Interrompe as execuções em andamento que foram canceladas pela API. */
    @Scheduled(fixedDelayString = "\${cronos.worker.cancel-check-interval-ms:1000}")
    fun checkCancelled() {
        if (inFlight.isEmpty()) return
        for (id in repository.findCancelled(workerId, inFlight.keys.toList())) {
            inFlight[id]?.let {
                log.info("Execução {} cancelada via API: interrompendo", id)
                it.cancel()
            }
        }
    }

    @Scheduled(fixedDelayString = "\${cronos.worker.reaper-interval-ms:5000}")
    fun reap() {
        val count = repository.reapExpired(clock.instant())
        if (count > 0) {
            reaped.increment(count.toDouble())
            log.warn("Reaper recuperou {} execução(ões) com lease expirado", count)
        }
    }

    private fun run(execution: ClaimedExecution, handle: InFlight) {
        val sample = Timer.start(meters)
        val result = try {
            webhook.call(execution) { handle.attach(it) }
        } catch (e: Exception) {
            AttemptResult.Failure("erro inesperado: ${e.message}", retryable = true)
        }
        sample.stop(meters.timer("cronos.webhook.duration",
            "result", if (result is AttemptResult.Success) "success" else "failure"))

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
            count("error")
            return
        }

        if (updated == 0) {
            // Cancelada pela API ou recuperada pelo reaper depois que o lease venceu.
            log.info("Execução {} não está mais RUNNING para {}: resultado descartado", execution.id, workerId)
            count("discarded")
            return
        }
        when (outcome) {
            Outcome.Succeeded -> {
                count("succeeded")
                log.debug("Execução {} concluída (tentativa {})", execution.id, execution.attempt)
            }
            is Outcome.Retry -> {
                count("retry")
                log.info("Execução {} falhou na tentativa {}/{}: {}. Nova tentativa em {}",
                    execution.id, execution.attempt, execution.maxAttempts, outcome.error, outcome.runAfter)
            }
            is Outcome.Dead -> {
                count("dead")
                log.warn("Execução {} DEAD após {} tentativas: {}", execution.id, execution.attempt, outcome.error)
            }
            is Outcome.Failed -> {
                count("failed")
                log.warn("Execução {} FAILED sem retentativa: {}", execution.id, outcome.error)
            }
        }
    }

    /**
     * Cancelar aborta só a chamada HTTP, sem interromper a thread: um interrupt numa
     * virtual thread fecharia a conexão JDBC se chegasse durante a gravação do resultado.
     */
    private class InFlight {
        @Volatile private var call: CompletableFuture<*>? = null
        @Volatile private var cancelled = false

        fun attach(future: CompletableFuture<*>) {
            call = future
            if (cancelled) future.cancel(true) // cancelamento chegou antes do envio
        }

        fun cancel() {
            cancelled = true
            call?.cancel(true)
        }
    }

    private fun count(outcome: String) = meters.counter("cronos.worker.executions", "outcome", outcome).increment()

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
