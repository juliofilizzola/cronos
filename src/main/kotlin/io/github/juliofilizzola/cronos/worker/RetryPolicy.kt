package io.github.juliofilizzola.cronos.worker

import java.time.Duration
import java.time.Instant
import kotlin.random.Random

/** Resultado de uma tentativa de execução. */
sealed interface AttemptResult {
    data object Success : AttemptResult

    /** [retryable] = false para erros que não mudam ao repetir (4xx, payload inválido). */
    data class Failure(val error: String, val retryable: Boolean) : AttemptResult
}

/** O que fazer com a execução depois da tentativa. */
sealed interface Outcome {
    data object Succeeded : Outcome
    data class Retry(val runAfter: Instant, val error: String) : Outcome
    /** Esgotou as tentativas. */
    data class Dead(val error: String) : Outcome
    /** Falha definitiva, sem retentativa. */
    data class Failed(val error: String) : Outcome
}

/**
 * Decide o destino de uma execução. Função pura (o aleatório é injetado),
 * no mesmo espírito do MisfireResolver.
 *
 * Backoff exponencial com "equal jitter": metade fixa e metade aleatória, para que
 * execuções que falharam juntas não voltem todas no mesmo instante.
 */
class RetryPolicy(
    private val base: Duration,
    private val max: Duration,
    private val random: Random = Random.Default,
) {
    fun decide(result: AttemptResult, attempt: Int, maxAttempts: Int, now: Instant): Outcome = when (result) {
        AttemptResult.Success -> Outcome.Succeeded
        is AttemptResult.Failure -> when {
            !result.retryable -> Outcome.Failed(result.error)
            attempt >= maxAttempts -> Outcome.Dead(result.error)
            else -> Outcome.Retry(now.plus(backoff(attempt)), result.error)
        }
    }

    /** Espera antes da tentativa seguinte à [attempt] (1 = primeira tentativa). */
    fun backoff(attempt: Int): Duration {
        val exp = base.toMillis() * (1L shl (attempt - 1).coerceIn(0, 30))
        val capped = exp.coerceAtMost(max.toMillis())
        val half = capped / 2
        return Duration.ofMillis(half + random.nextLong(half + 1))
    }
}
