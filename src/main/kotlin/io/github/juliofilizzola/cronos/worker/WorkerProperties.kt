package io.github.juliofilizzola.cronos.worker

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("cronos.worker")
data class WorkerProperties(
    val enabled: Boolean = true,
    /** Máximo de execuções simultâneas nesta instância. */
    val concurrency: Int = 16,
    val pollIntervalMs: Long = 500,
    /** Quanto tempo a execução fica reservada sem heartbeat antes do reaper liberá-la. */
    val lease: Duration = Duration.ofSeconds(30),
    val heartbeatIntervalMs: Long = 10_000,
    val reaperIntervalMs: Long = 5_000,
    val backoffBase: Duration = Duration.ofSeconds(1),
    val backoffMax: Duration = Duration.ofMinutes(5),
    /** Espera pelas execuções em andamento ao desligar. */
    val shutdownGrace: Duration = Duration.ofSeconds(30),
)
