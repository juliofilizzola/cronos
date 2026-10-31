package io.github.juliofilizzola.cronos

import io.github.juliofilizzola.cronos.api.CreateJobRequest
import io.github.juliofilizzola.cronos.domain.ConflictException
import io.github.juliofilizzola.cronos.domain.MisfirePolicy
import io.github.juliofilizzola.cronos.domain.ScheduleType
import io.github.juliofilizzola.cronos.persistence.ExecutionRepository
import io.github.juliofilizzola.cronos.scheduling.SchedulerLoop
import io.github.juliofilizzola.cronos.service.ExecutionService
import io.github.juliofilizzola.cronos.service.JobService
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Collections
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Coordenação pelo PostgreSQL: claim, reaper, scheduler concorrente e transições da API.
 * O worker fica desligado e o scheduler só roda quando o teste chama tick(),
 * para nada concorrer com o que está sendo medido.
 */
@Import(TestcontainersConfiguration::class)
@SpringBootTest(properties = ["cronos.worker.enabled=false", "cronos.scheduler.poll-interval-ms=3600000"])
class ConcurrencyIntegrationTest {

    @Autowired lateinit var jobs: JobService
    @Autowired lateinit var executionService: ExecutionService
    @Autowired lateinit var executions: ExecutionRepository
    @Autowired lateinit var scheduler: SchedulerLoop
    @Autowired lateinit var jdbc: JdbcTemplate
    @Autowired lateinit var jsonMapper: JsonMapper

    private fun job(
        type: ScheduleType = ScheduleType.ONCE,
        expr: String = "2099-01-01T00:00:00Z", // nunca vence: as execuções são criadas pelo teste
        maxAttempts: Int = 3,
        allowOverlap: Boolean = true,
        policy: MisfirePolicy = MisfirePolicy.FIRE_ONCE,
        misfireThresholdSeconds: Int = 60,
    ): UUID = jobs.create(
        CreateJobRequest(
            name = "it-${UUID.randomUUID()}", scheduleType = type, scheduleExpr = expr,
            payload = jsonMapper.readTree("""{"url":"http://localhost:1/x"}"""),
            maxAttempts = maxAttempts, allowOverlap = allowOverlap, misfirePolicy = policy,
            misfireThresholdSeconds = misfireThresholdSeconds,
        )
    ).id

    private fun pending(jobId: UUID, n: Int): Set<UUID> {
        val base = Instant.now().truncatedTo(ChronoUnit.SECONDS).minusSeconds(n.toLong())
        return (0 until n).map { i ->
            UUID.randomUUID().also { executions.insert(it, jobId, base.plusSeconds(i.toLong()), base, "SCHEDULER") }
        }.toSet()
    }

    private fun status(id: UUID) = executions.findById(id)!!.status

    @Test
    fun `claims concorrentes nunca entregam a mesma execucao a dois workers`() {
        val mine = pending(job(), 300)
        val claimed = Collections.synchronizedList(mutableListOf<UUID>())
        val pool = Executors.newFixedThreadPool(8)
        val start = CountDownLatch(1)
        repeat(8) { w ->
            pool.submit {
                start.await()
                while (true) {
                    val now = Instant.now()
                    val batch = executions.claim("worker-$w", 7, now, now.plusSeconds(60))
                    if (batch.isEmpty()) break
                    claimed += batch.map { it.id }
                }
            }
        }
        start.countDown()
        pool.shutdown()
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS))

        val claimedMine = claimed.filter { it in mine }
        assertEquals(mine.size, claimedMine.size, "toda execução deve ser reservada exatamente uma vez")
        assertEquals(mine, claimedMine.toSet())
        mine.forEach { assertEquals("RUNNING", status(it)) }
    }

    @Test
    fun `resultado de um worker que perdeu a reserva e descartado`() {
        val id = pending(job(), 1).single()
        val now = Instant.now()
        executions.claim("dono", 1000, now, now.plusSeconds(60))

        assertEquals(0, executions.finish(id, "intruso", "SUCCEEDED", now, null, null))
        assertEquals("RUNNING", status(id))
        assertEquals(1, executions.finish(id, "dono", "SUCCEEDED", now, null, null))
        assertEquals("SUCCEEDED", status(id))
    }

    @Test
    fun `reaper devolve para a fila ou marca DEAD quando o lease vence`() {
        val retryable = pending(job(maxAttempts = 3), 1).single()
        val exhausted = pending(job(maxAttempts = 1), 1).single()
        val now = Instant.now()
        executions.claim("morto", 1000, now, now.minusSeconds(1)) // lease já vencido

        assertTrue(executions.reapExpired(now) >= 2)
        assertEquals("PENDING", status(retryable))
        assertEquals("DEAD", status(exhausted))
        assertTrue(executions.findById(retryable)!!.lastError!!.contains("lease expirado"))
    }

    @Test
    fun `heartbeat impede o reaper de recuperar execucao viva`() {
        val id = pending(job(), 1).single()
        val now = Instant.now()
        executions.claim("vivo", 1000, now, now.plusSeconds(1))
        executions.extendLeases("vivo", now.plusSeconds(60))

        executions.reapExpired(now.plusSeconds(5))
        assertEquals("RUNNING", status(id))
    }

    @Test
    fun `varios schedulers ao mesmo tempo nao duplicam nem perdem ocorrencias`() {
        // 20 jobs a cada segundo, parados há 30s: FIRE_ALL deve recuperar cada segundo uma única vez.
        val ids = (1..20).map { job(ScheduleType.INTERVAL, "PT1S", policy = MisfirePolicy.FIRE_ALL, misfireThresholdSeconds = 0) }
        val from = Instant.now().truncatedTo(ChronoUnit.SECONDS).minusSeconds(30)
        ids.forEach { jdbc.update("UPDATE jobs SET next_fire_at = ? WHERE id = ?", java.sql.Timestamp.from(from), it) }

        val pool = Executors.newFixedThreadPool(4)
        val start = CountDownLatch(1)
        repeat(4) { pool.submit { start.await(); repeat(3) { scheduler.tick() } } }
        start.countDown()
        pool.shutdown()
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS))

        for (id in ids) {
            val times = jdbc.queryForList(
                "SELECT scheduled_for FROM executions WHERE job_id = ? ORDER BY scheduled_for",
                java.sql.Timestamp::class.java, id,
            ).map { it!!.toInstant() }
            assertEquals(from, times.first(), "primeira ocorrência perdida deve ser recuperada")
            assertEquals(times.size, times.toSet().size, "ocorrência duplicada")
            times.zipWithNext().forEach { (a, b) ->
                assertEquals(Duration.ofSeconds(1), Duration.between(a, b), "buraco entre $a e $b")
            }
            assertTrue(times.size >= 30, "esperava ao menos 30 ocorrências, veio ${times.size}")
        }
    }

    @Test
    fun `disparo manual, cancelamento e retentativa respeitam as transicoes`() {
        val jobId = job(allowOverlap = false)
        val manual = executionService.trigger(jobId)
        assertEquals("MANUAL", manual.triggeredBy)
        assertEquals("PENDING", manual.status)

        // Sem sobreposição: o segundo disparo é recusado enquanto o primeiro está ativo.
        assertThrows<ConflictException> { executionService.trigger(jobId) }

        assertEquals("CANCELLED", executionService.cancel(manual.id).status)
        assertThrows<ConflictException> { executionService.cancel(manual.id) }
        assertThrows<ConflictException> { executionService.retry(manual.id) }

        // Depois de cancelado, pode disparar de novo.
        val second = executionService.trigger(jobId)
        val now = Instant.now()
        executions.claim("w", 1000, now, now.plusSeconds(60))
        executions.finish(second.id, "w", "DEAD", now, null, "HTTP 500")

        val retried = executionService.retry(second.id)
        assertEquals("PENDING", retried.status)
        assertEquals(0, retried.attempt)
        assertEquals(second.id, retried.id, "retentativa mantém o id (mesma chave de idempotência)")
    }
}
