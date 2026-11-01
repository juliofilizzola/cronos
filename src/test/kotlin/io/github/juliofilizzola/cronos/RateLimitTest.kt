package io.github.juliofilizzola.cronos.security

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.mock.web.MockFilterChain
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.mock.web.MockHttpServletResponse
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** Relógio controlado pelo teste, em nanossegundos. */
private class FakeClock {
    var nanos = 0L
    fun advanceMs(ms: Long) { nanos += ms * 1_000_000 }
}

class TokenBucketLimiterTest {

    private val clock = FakeClock()
    private val limiter = TokenBucketLimiter(capacity = 5.0, refillPerSecond = 2.0, nanoTime = { clock.nanos })

    @Test
    fun `permite a rajada e depois bloqueia`() {
        repeat(5) { assertTrue(limiter.tryConsume("a").allowed, "requisição ${it + 1} da rajada") }
        val blocked = limiter.tryConsume("a")
        assertFalse(blocked.allowed)
        assertEquals(1, blocked.retryAfterSeconds) // 2 fichas/s: falta 1 ficha = 0,5 s, arredonda para 1
    }

    @Test
    fun `recarrega na vazao configurada`() {
        repeat(5) { limiter.tryConsume("a") }
        clock.advanceMs(500) // 2/s * 0,5 s = 1 ficha
        assertTrue(limiter.tryConsume("a").allowed)
        assertFalse(limiter.tryConsume("a").allowed)
        clock.advanceMs(60_000) // muito tempo: volta só até a capacidade
        repeat(5) { assertTrue(limiter.tryConsume("a").allowed) }
        assertFalse(limiter.tryConsume("a").allowed)
    }

    @Test
    fun `clientes sao independentes`() {
        repeat(5) { limiter.tryConsume("a") }
        assertFalse(limiter.tryConsume("a").allowed)
        assertTrue(limiter.tryConsume("b").allowed)
    }

    @Test
    fun `remaining diminui a cada requisicao`() {
        assertEquals(listOf(4, 3, 2, 1, 0), (1..5).map { limiter.tryConsume("a").remaining })
    }

    @Test
    fun `sob concorrencia nao libera mais que a capacidade`() {
        val big = TokenBucketLimiter(capacity = 100.0, refillPerSecond = 0.001, nanoTime = { clock.nanos })
        val allowed = AtomicInteger()
        val pool = Executors.newFixedThreadPool(16)
        val start = CountDownLatch(1)
        repeat(1000) { pool.submit { start.await(); if (big.tryConsume("x").allowed) allowed.incrementAndGet() } }
        start.countDown()
        pool.shutdown()
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS))
        assertEquals(100, allowed.get())
    }

    @Test
    fun `limita a memoria descartando clientes ociosos`() {
        val small = TokenBucketLimiter(capacity = 2.0, refillPerSecond = 1.0, maxClients = 100, nanoTime = { clock.nanos })
        repeat(1000) { small.tryConsume("ip-$it") }
        assertTrue(small.trackedClients() <= 100, "rastreando ${small.trackedClients()} clientes")
    }
}

class ApiKeyFilterRateLimitTest {

    private val clock = FakeClock()
    private val limited = mutableListOf<String>()

    private fun filter(limits: RateLimitProperties) = ApiKeyFilter(
        SecurityProperties(apiKeys = listOf("chave-a", "chave-b"), rateLimit = limits),
        onLimited = { limited += it },
        nanoTime = { clock.nanos },
    )

    private fun call(
        f: ApiKeyFilter, key: String?, ip: String = "203.0.113.1", forwardedFor: String? = null,
    ): MockHttpServletResponse {
        val request = MockHttpServletRequest("GET", "/jobs").apply {
            remoteAddr = ip
            key?.let { addHeader("Authorization", "Bearer $it") }
            forwardedFor?.let { addHeader("X-Forwarded-For", it) }
        }
        val response = MockHttpServletResponse()
        f.doFilter(request, response, MockFilterChain())
        return response
    }

    @Test
    fun `chave valida e limitada pela rajada e recebe 429 com Retry-After`() {
        val f = filter(RateLimitProperties(requestsPerSecond = 10.0, burst = 3))
        val statuses = (1..4).map { call(f, "chave-a") }
        assertEquals(listOf(200, 200, 200, 429), statuses.map { it.status })
        assertEquals("1", statuses.last().getHeader("Retry-After"))
        assertEquals("3", statuses.first().getHeader("X-RateLimit-Limit"))
        assertEquals("2", statuses.first().getHeader("X-RateLimit-Remaining"))
        assertTrue(statuses.last().contentAsString.contains("Too Many Requests"))
        assertEquals(listOf("key"), limited)

        clock.advanceMs(100) // 10/s: 1 ficha
        assertEquals(200, call(f, "chave-a").status)
    }

    @Test
    fun `cada chave tem o proprio balde`() {
        val f = filter(RateLimitProperties(burst = 1))
        assertEquals(200, call(f, "chave-a").status)
        assertEquals(429, call(f, "chave-a").status)
        assertEquals(200, call(f, "chave-b").status)
    }

    @Test
    fun `forca bruta do mesmo IP passa de 401 para 429`() {
        val f = filter(RateLimitProperties(failedAuthBurst = 3, failedAuthPerMinute = 6.0))
        val statuses = (1..5).map { call(f, "chute-$it").status }
        assertEquals(listOf(401, 401, 401, 429, 429), statuses)
        assertEquals("10", call(f, "chute-x").getHeader("Retry-After")) // 6/min = 1 a cada 10 s
        assertEquals(401, call(f, null, ip = "198.51.100.7").status, "outro IP não é afetado")
        clock.advanceMs(10_000)
        assertEquals(401, call(f, "chute-y").status, "recarregou uma tentativa")
    }

    @Test
    fun `chave valida nao e bloqueada pelos erros de outro cliente no mesmo IP`() {
        val f = filter(RateLimitProperties(failedAuthBurst = 2))
        repeat(5) { call(f, "chute") }
        assertEquals(429, call(f, "chute").status)
        assertEquals(200, call(f, "chave-a").status)
    }

    @Test
    fun `X-Forwarded-For so conta quando configurado`() {
        val ignoring = filter(RateLimitProperties(failedAuthBurst = 2))
        // Sem confiar no header, trocar o X-Forwarded-For não escapa do limite do IP real.
        val spoofed = (1..3).map { call(ignoring, "chute", forwardedFor = "10.0.0.$it").status }
        assertEquals(listOf(401, 401, 429), spoofed)

        val trusting = filter(RateLimitProperties(failedAuthBurst = 2, trustForwardedFor = true))
        repeat(2) { call(trusting, "chute", forwardedFor = "198.51.100.1, 10.0.0.1") }
        assertEquals(429, call(trusting, "chute", forwardedFor = "198.51.100.1").status)
        assertEquals(401, call(trusting, "chute", forwardedFor = "198.51.100.2").status)
    }

    @Test
    fun `desligado nao limita`() {
        val f = filter(RateLimitProperties(enabled = false, burst = 1, failedAuthBurst = 1))
        repeat(20) { assertEquals(200, call(f, "chave-a").status) }
        repeat(20) { assertEquals(401, call(f, "chute").status) }
    }
}
