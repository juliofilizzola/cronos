package io.github.juliofilizzola.cronos.worker

import io.github.juliofilizzola.cronos.persistence.ClaimedExecution
import io.github.juliofilizzola.cronos.security.UrlGuard
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/** Formato do payload de um job: `{ "url": ..., "method": "POST", "headers": {...}, "body": ... }`. */
data class WebhookPayload(
    val url: String? = null,
    val method: String = "POST",
    val headers: Map<String, String> = emptyMap(),
    val body: JsonNode? = null,
)

/** Faz a chamada HTTP de uma execução e traduz o resultado para [AttemptResult]. */
class WebhookClient(
    private val jsonMapper: JsonMapper,
    private val guard: UrlGuard,
    private val http: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .followRedirects(HttpClient.Redirect.NEVER)
        .build(),
) {

    /**
     * [onSend] recebe a chamada em andamento: cancelá-la aborta a requisição
     * (é assim que o cancelamento via API interrompe uma execução RUNNING).
     */
    fun call(execution: ClaimedExecution, onSend: (CompletableFuture<*>) -> Unit = {}): AttemptResult {
        val request = try {
            buildRequest(execution)
        } catch (e: Exception) {
            return AttemptResult.Failure("payload inválido: ${e.message}", retryable = false)
        }
        // Checado a cada chamada, não só na criação: o DNS do host pode ter mudado desde então.
        guard.check(request.uri())?.let { return AttemptResult.Failure(it.reason, it.retryable) }

        // Prazo total (conexão + resposta + corpo), não só até os headers chegarem.
        val future = http.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        onSend(future)
        return try {
            val response = future.get(execution.timeoutSeconds.toLong(), TimeUnit.SECONDS)
            classify(response.statusCode(), response.body())
        } catch (e: TimeoutException) {
            future.cancel(true)
            AttemptResult.Failure("timeout após ${execution.timeoutSeconds}s", retryable = true)
        } catch (e: ExecutionException) {
            val cause = e.cause ?: e
            val detail = cause.message?.let { ": $it" } ?: ""
            AttemptResult.Failure("${cause.javaClass.simpleName}$detail", retryable = true)
        } catch (e: CancellationException) {
            AttemptResult.Failure("cancelada", retryable = false)
        } catch (e: InterruptedException) {
            future.cancel(true)
            Thread.currentThread().interrupt()
            AttemptResult.Failure("interrompida (worker desligando)", retryable = true)
        }
    }

    private fun buildRequest(execution: ClaimedExecution): HttpRequest {
        val payload = jsonMapper.readValue(execution.payload, WebhookPayload::class.java)
        val url = requireNotNull(payload.url?.takeIf { it.isNotBlank() }) { "campo 'url' ausente" }
        val uri = URI.create(url)
        require(uri.scheme == "http" || uri.scheme == "https") { "url deve ser http ou https" }

        val body = payload.body?.let { jsonMapper.writeValueAsString(it) }
        val publisher = body?.let { HttpRequest.BodyPublishers.ofString(it) } ?: HttpRequest.BodyPublishers.noBody()

        val builder = HttpRequest.newBuilder(uri)
            .method(payload.method.uppercase(), publisher)
            .header("Idempotency-Key", execution.id.toString())
            .header("X-Cronos-Execution-Id", execution.id.toString())
            .header("X-Cronos-Job-Id", execution.jobId.toString())
            .header("X-Cronos-Attempt", execution.attempt.toString())
            .header("X-Cronos-Scheduled-For", execution.scheduledFor.toString())
        if (body != null) builder.header("Content-Type", "application/json")
        payload.headers.forEach { (k, v) -> builder.setHeader(k, v) }
        return builder.build()
    }

    internal companion object {
        /** 2xx é sucesso. 408, 429 e 5xx são temporários; o resto dos 4xx não melhora repetindo. */
        fun classify(status: Int, body: String?): AttemptResult = when {
            status in 200..299 -> AttemptResult.Success
            status == 408 || status == 429 || status >= 500 ->
                AttemptResult.Failure(describe(status, body), retryable = true)
            else -> AttemptResult.Failure(describe(status, body), retryable = false)
        }

        private fun describe(status: Int, body: String?) =
            "HTTP $status" + (body?.takeIf { it.isNotBlank() }?.let { ": ${it.take(200)}" } ?: "")
    }
}
