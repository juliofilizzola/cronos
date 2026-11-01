package io.github.juliofilizzola.cronos.security

import java.util.concurrent.ConcurrentHashMap
import kotlin.math.ceil
import kotlin.math.min

/**
 * Rate limit por token bucket, em memória: cada cliente tem um balde com [capacity] fichas
 * que se recarrega a [refillPerSecond] fichas por segundo; cada requisição gasta uma.
 * [capacity] é a rajada tolerada, [refillPerSecond] a vazão sustentada.
 *
 * O estado é por instância: com N instâncias atrás de um balanceador, o limite efetivo
 * é até N vezes maior. O relógio é injetado para os testes.
 */
class TokenBucketLimiter(
    private val capacity: Double,
    private val refillPerSecond: Double,
    /** Teto de clientes rastreados, para que muitos IPs diferentes não esgotem a memória. */
    private val maxClients: Int = 10_000,
    private val nanoTime: () -> Long = System::nanoTime,
) {
    init {
        require(capacity >= 1) { "capacity deve ser >= 1" }
        require(refillPerSecond > 0) { "refillPerSecond deve ser > 0" }
    }

    data class Decision(val allowed: Boolean, val remaining: Int, val retryAfterSeconds: Long)

    private class Bucket(var tokens: Double, var updatedAt: Long)

    private val buckets = ConcurrentHashMap<String, Bucket>()

    val limit: Int get() = capacity.toInt()

    fun tryConsume(client: String): Decision {
        val now = nanoTime()
        if (buckets.size >= maxClients && !buckets.containsKey(client)) evict(now)
        val bucket = buckets.computeIfAbsent(client) { Bucket(capacity, now) }
        synchronized(bucket) {
            refill(bucket, now)
            if (bucket.tokens >= 1) {
                bucket.tokens -= 1
                return Decision(true, bucket.tokens.toInt(), 0)
            }
            val wait = ceil((1 - bucket.tokens) / refillPerSecond).toLong().coerceAtLeast(1)
            return Decision(false, 0, wait)
        }
    }

    internal fun trackedClients() = buckets.size

    private fun refill(bucket: Bucket, now: Long) {
        val elapsed = (now - bucket.updatedAt).coerceAtLeast(0) / 1e9
        bucket.tokens = min(capacity, bucket.tokens + elapsed * refillPerSecond)
        bucket.updatedAt = now
    }

    /**
     * Remove primeiro os baldes cheios (clientes ociosos: apagá-los não muda nada, porque
     * um balde novo também nasce cheio). Se ainda faltar espaço, descarta 10% arbitrários.
     */
    private fun evict(now: Long) {
        buckets.entries.removeIf { (_, b) -> synchronized(b) { refill(b, now); b.tokens >= capacity } }
        if (buckets.size >= maxClients) {
            buckets.keys.take(maxOf(1, maxClients / 10)).forEach(buckets::remove)
        }
    }
}
