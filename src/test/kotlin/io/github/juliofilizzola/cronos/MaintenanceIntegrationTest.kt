package io.github.juliofilizzola.cronos

import io.github.juliofilizzola.cronos.api.CreateJobRequest
import io.github.juliofilizzola.cronos.domain.InvalidPayloadException
import io.github.juliofilizzola.cronos.domain.NotFoundException
import io.github.juliofilizzola.cronos.domain.ScheduleType
import io.github.juliofilizzola.cronos.persistence.ExecutionRepository
import io.github.juliofilizzola.cronos.scheduling.ExecutionRetention
import io.github.juliofilizzola.cronos.service.JobService
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import tools.jackson.databind.json.JsonMapper
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.util.UUID

/** DELETE de jobs, retenção do histórico e validação de destino. Mesmo contexto do ConcurrencyIntegrationTest. */
@Import(TestcontainersConfiguration::class)
@SpringBootTest(properties = [
    "cronos.worker.enabled=false", "cronos.scheduler.poll-interval-ms=3600000",
    "cronos.security.api-keys=test-key", "cronos.webhook.allow-private-networks=true",
])
class MaintenanceIntegrationTest {

    @Autowired lateinit var jobs: JobService
    @Autowired lateinit var executions: ExecutionRepository
    @Autowired lateinit var retention: ExecutionRetention
    @Autowired lateinit var jdbc: JdbcTemplate
    @Autowired lateinit var jsonMapper: JsonMapper

    private fun job(url: String = "http://localhost:1/x") = jobs.create(
        CreateJobRequest(
            name = "maint-${UUID.randomUUID()}", scheduleType = ScheduleType.ONCE,
            scheduleExpr = "2099-01-01T00:00:00Z", payload = jsonMapper.readTree("""{"url":"$url"}"""),
        )
    ).id

    private fun execution(jobId: UUID, status: String, finishedAgo: Duration?): UUID {
        val id = UUID.randomUUID()
        val at = Instant.now().minusSeconds((Math.random() * 1_000_000).toLong())
        executions.insert(id, jobId, at, at, "SCHEDULER")
        jdbc.update(
            "UPDATE executions SET status = ?, finished_at = ? WHERE id = ?",
            status, finishedAgo?.let { Timestamp.from(Instant.now().minus(it)) }, id,
        )
        return id
    }

    private fun exists(id: UUID) = executions.findById(id) != null

    @Test
    fun `DELETE apaga o job e todo o historico`() {
        val jobId = job()
        val ids = listOf("PENDING", "RUNNING", "SUCCEEDED", "DEAD").map { execution(jobId, it, null) }

        jobs.delete(jobId)

        assertThrows<NotFoundException> { jobs.get(jobId) }
        ids.forEach { assertFalse(exists(it)) }
        assertThrows<NotFoundException> { jobs.delete(jobId) }
    }

    @Test
    fun `retencao apaga so execucoes terminadas e antigas`() {
        val jobId = job()
        val old = listOf("SUCCEEDED", "FAILED", "DEAD", "CANCELLED").map { execution(jobId, it, Duration.ofDays(31)) }
        val recent = execution(jobId, "SUCCEEDED", Duration.ofDays(1))
        val pending = execution(jobId, "PENDING", null)
        val running = execution(jobId, "RUNNING", null)

        assertTrue(retention.purge() >= old.size)

        old.forEach { assertFalse(exists(it), "execução terminada há 31 dias deveria ter sido apagada") }
        listOf(recent, pending, running).forEach { assertTrue(exists(it)) }
        assertNotNull(jobs.get(jobId), "a retenção não apaga o job")
    }

    @Test
    fun `retencao trabalha em lotes ate acabar`() {
        val jobId = job()
        val old = (1..2500).map { execution(jobId, "SUCCEEDED", Duration.ofDays(40)) }
        assertTrue(retention.purge() >= old.size) // batch-size 1000: três rodadas
        assertEquals(0, old.count(::exists))
    }
}

/** Com a proteção de SSRF ligada (padrão), a criação recusa destinos internos. */
@Import(TestcontainersConfiguration::class)
@SpringBootTest(properties = [
    "cronos.worker.enabled=false", "cronos.scheduler.poll-interval-ms=3600000",
    "cronos.security.api-keys=test-key",
])
class SsrfIntegrationTest {

    @Autowired lateinit var jobs: JobService
    @Autowired lateinit var jsonMapper: JsonMapper

    private fun create(url: String) = jobs.create(
        CreateJobRequest(
            name = "ssrf", scheduleType = ScheduleType.ONCE, scheduleExpr = "2099-01-01T00:00:00Z",
            payload = jsonMapper.readTree("""{"url":"$url"}"""),
        )
    )

    @Test
    fun `criacao recusa destinos internos`() {
        listOf("http://localhost:8080/jobs", "http://127.0.0.1/", "http://169.254.169.254/latest/meta-data",
            "http://10.0.0.1/", "http://[::1]/").forEach { url ->
            val e = assertThrows<InvalidPayloadException>(url) { create(url) }
            assertTrue(e.message!!.contains("bloqueado"), "${e.message}")
        }
    }

    @Test
    fun `criacao aceita destino publico`() {
        assertNotNull(create("https://93.184.216.34/hook").id)
    }
}
