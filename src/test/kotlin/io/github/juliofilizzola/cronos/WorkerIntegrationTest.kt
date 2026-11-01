package io.github.juliofilizzola.cronos

import com.sun.net.httpserver.HttpServer
import io.github.juliofilizzola.cronos.api.CreateJobRequest
import io.github.juliofilizzola.cronos.domain.ExecutionView
import io.github.juliofilizzola.cronos.domain.ScheduleType
import io.github.juliofilizzola.cronos.service.ExecutionService
import io.github.juliofilizzola.cronos.service.JobService
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import tools.jackson.databind.json.JsonMapper
import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors

/** Worker de verdade contra um servidor HTTP local, com o PostgreSQL do Testcontainers. */
@Import(TestcontainersConfiguration::class)
@SpringBootTest(
    properties = [
        "cronos.worker.backoff-base=100ms",
        "cronos.worker.poll-interval-ms=100",
        "cronos.worker.cancel-check-interval-ms=200",
        "cronos.security.api-keys=test-key",
        "cronos.webhook.allow-private-networks=true", // o servidor de teste roda em 127.0.0.1
    ]
)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkerIntegrationTest {

    @Autowired lateinit var jobs: JobService
    @Autowired lateinit var executions: ExecutionService
    @Autowired lateinit var jsonMapper: JsonMapper

    private lateinit var server: HttpServer
    private val hits = ConcurrentHashMap<String, CopyOnWriteArrayList<Map<String, String>>>()

    @BeforeAll
    fun startServer() {
        server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.executor = Executors.newVirtualThreadPerTaskExecutor()
        server.createContext("/") { ex ->
            val path = ex.requestURI.path
            val headers = ex.requestHeaders.entries.associate { it.key.lowercase() to it.value.first() }
            hits.computeIfAbsent(path) { CopyOnWriteArrayList() } += headers
            val status = when (path) {
                "/ok" -> 200
                "/fail" -> 500
                "/notfound" -> 404
                "/slow" -> { Thread.sleep(15_000); 200 }
                else -> 200
            }
            ex.sendResponseHeaders(status, -1)
            ex.close()
        }
        server.start()
    }

    @AfterAll
    fun stopServer() = server.stop(0)

    private fun url(path: String) = "http://127.0.0.1:${server.address.port}$path"

    private fun jobFor(path: String, maxAttempts: Int = 3, timeoutSeconds: Int = 5): UUID = jobs.create(
        CreateJobRequest(
            name = "worker-it-$path", scheduleType = ScheduleType.ONCE, scheduleExpr = "2099-01-01T00:00:00Z",
            payload = jsonMapper.readTree("""{"url":"${url(path)}","method":"POST","body":{"a":1}}"""),
            maxAttempts = maxAttempts, timeoutSeconds = timeoutSeconds, allowOverlap = true,
        )
    ).id

    private fun await(id: UUID, timeoutMs: Long = 15_000, predicate: (ExecutionView) -> Boolean): ExecutionView {
        val deadline = System.currentTimeMillis() + timeoutMs
        var last = executions.get(id)
        while (!predicate(last)) {
            assertTrue(System.currentTimeMillis() < deadline, "tempo esgotado; último estado: $last")
            Thread.sleep(50)
            last = executions.get(id)
        }
        return last
    }

    @Test
    fun `executa o webhook e envia a chave de idempotencia`() {
        val exec = executions.trigger(jobFor("/ok"))
        val done = await(exec.id) { it.status == "SUCCEEDED" }

        assertEquals(1, done.attempt)
        val call = hits["/ok"]!!.single { it["idempotency-key"] == exec.id.toString() }
        assertEquals("1", call["x-cronos-attempt"])
        assertEquals("application/json", call["content-type"])
    }

    @Test
    fun `erro temporario e retentado com backoff ate virar DEAD`() {
        val exec = executions.trigger(jobFor("/fail", maxAttempts = 3))
        val dead = await(exec.id) { it.status == "DEAD" }

        assertEquals(3, dead.attempt)
        assertTrue(dead.lastError!!.startsWith("HTTP 500"))
        assertEquals(listOf("1", "2", "3"),
            hits["/fail"]!!.filter { it["idempotency-key"] == exec.id.toString() }.map { it["x-cronos-attempt"] })
    }

    @Test
    fun `erro definitivo nao e retentado`() {
        val exec = executions.trigger(jobFor("/notfound"))
        val failed = await(exec.id) { it.status == "FAILED" }
        assertEquals(1, failed.attempt)
        assertTrue(failed.lastError!!.startsWith("HTTP 404"))
    }

    @Test
    fun `timeout conta como falha temporaria`() {
        val exec = executions.trigger(jobFor("/slow", maxAttempts = 1, timeoutSeconds = 1))
        val dead = await(exec.id) { it.status == "DEAD" }
        assertEquals("timeout após 1s", dead.lastError)
    }

    @Test
    fun `cancelar uma execucao em andamento interrompe a chamada`() {
        val exec = executions.trigger(jobFor("/slow", timeoutSeconds = 60))
        await(exec.id) { it.status == "RUNNING" }

        val start = System.currentTimeMillis()
        executions.cancel(exec.id)
        Thread.sleep(1500) // tempo para o worker perceber e abortar

        val after = executions.get(exec.id)
        assertEquals("CANCELLED", after.status)
        assertEquals("cancelada via API", after.lastError)
        assertTrue(System.currentTimeMillis() - start < 15_000)
    }

    @Test
    fun `retentar uma execucao DEAD a executa de novo com o mesmo id`() {
        val exec = executions.trigger(jobFor("/fail", maxAttempts = 1))
        await(exec.id) { it.status == "DEAD" }

        executions.retry(exec.id)
        val again = await(exec.id) { it.status == "DEAD" && it.attempt == 1 && it.finishedAt != null }
        assertEquals(exec.id, again.id)
        assertEquals(2, hits["/fail"]!!.count { it["idempotency-key"] == exec.id.toString() })
    }
}
