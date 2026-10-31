package io.github.juliofilizzola.cronos.worker

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import kotlin.random.Random

class RetryPolicyTest {

    private val policy = RetryPolicy(Duration.ofSeconds(1), Duration.ofMinutes(5), Random(42))
    private val now = Instant.parse("2026-10-24T10:00:00Z")
    private val temporary = AttemptResult.Failure("HTTP 500", retryable = true)

    @Test
    fun `sucesso conclui a execucao`() {
        assertEquals(Outcome.Succeeded, policy.decide(AttemptResult.Success, 1, 3, now))
    }

    @Test
    fun `falha temporaria com tentativas restantes agenda retentativa no futuro`() {
        val outcome = policy.decide(temporary, attempt = 1, maxAttempts = 3, now = now)
        assertTrue(outcome is Outcome.Retry)
        assertTrue((outcome as Outcome.Retry).runAfter.isAfter(now))
        assertEquals("HTTP 500", outcome.error)
    }

    @Test
    fun `falha temporaria na ultima tentativa vira DEAD`() {
        assertEquals(Outcome.Dead("HTTP 500"), policy.decide(temporary, attempt = 3, maxAttempts = 3, now = now))
    }

    @Test
    fun `falha definitiva nao e retentada`() {
        val outcome = policy.decide(AttemptResult.Failure("HTTP 404", retryable = false), 1, 3, now)
        assertEquals(Outcome.Failed("HTTP 404"), outcome)
    }

    @Test
    fun `backoff dobra a cada tentativa, com jitter entre metade e o valor cheio`() {
        for (attempt in 1..5) {
            val full = 1000L shl (attempt - 1) // 1s, 2s, 4s, 8s, 16s
            repeat(50) {
                val ms = policy.backoff(attempt).toMillis()
                assertTrue(ms in full / 2..full, "tentativa $attempt: $ms fora de [${full / 2}, $full]")
            }
        }
    }

    @Test
    fun `backoff respeita o teto`() {
        repeat(50) {
            val ms = policy.backoff(30).toMillis()
            assertTrue(ms in 150_000L..300_000L, "$ms fora de [150s, 300s]")
        }
    }

    @Test
    fun `classificacao de status HTTP`() {
        assertEquals(AttemptResult.Success, WebhookClient.classify(204, null))
        listOf(408, 429, 500, 503).forEach {
            assertTrue((WebhookClient.classify(it, null) as AttemptResult.Failure).retryable, "$it deveria ser temporário")
        }
        listOf(400, 401, 404, 422).forEach {
            assertFalse((WebhookClient.classify(it, null) as AttemptResult.Failure).retryable, "$it deveria ser definitivo")
        }
    }
}
