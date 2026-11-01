package io.github.juliofilizzola.cronos.scheduling

import io.github.juliofilizzola.cronos.persistence.ExecutionRepository
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration

@ConfigurationProperties("cronos.retention")
data class RetentionProperties(
    val enabled: Boolean = true,
    /** Execuções terminadas (SUCCEEDED, FAILED, DEAD, CANCELLED) mais antigas que isso são apagadas. */
    val executionsMaxAge: Duration = Duration.ofDays(30),
    val intervalMs: Long = 3_600_000,
    val batchSize: Int = 1000,
)

/**
 * Limpa o histórico de execuções. Só apaga o que já terminou: PENDING e RUNNING nunca saem.
 * Roda em lotes pequenos até não sobrar nada vencido.
 */
@Component
@ConditionalOnProperty(prefix = "cronos.retention", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class ExecutionRetention(
    private val repository: ExecutionRepository,
    private val props: RetentionProperties,
    private val clock: Clock,
    meters: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val deleted = Counter.builder("cronos.retention.deleted")
        .description("Execuções apagadas pela retenção").register(meters)

    @Scheduled(fixedDelayString = "\${cronos.retention.interval-ms:3600000}", initialDelayString = "\${cronos.retention.initial-delay-ms:60000}")
    fun purge(): Int {
        val before = clock.instant().minus(props.executionsMaxAge)
        var total = 0
        do {
            val n = repository.deleteFinishedBefore(before, props.batchSize)
            total += n
        } while (n == props.batchSize)
        if (total > 0) {
            deleted.increment(total.toDouble())
            log.info("Retenção: {} execução(ões) terminadas antes de {} apagadas", total, before)
        }
        return total
    }
}
