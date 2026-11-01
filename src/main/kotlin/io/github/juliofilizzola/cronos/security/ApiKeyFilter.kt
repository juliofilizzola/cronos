package io.github.juliofilizzola.cronos.security

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.web.filter.OncePerRequestFilter
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/**
 * Exige uma API key em toda requisição, exceto nas rotas públicas.
 * Aceita `Authorization: Bearer <chave>` ou `X-API-Key: <chave>`.
 *
 * A comparação é feita sobre o SHA-256 das chaves com MessageDigest.isEqual (tempo constante),
 * para que o tempo de resposta não revele quantos caracteres da chave estão certos.
 *
 * Rate limit, com dois baldes independentes:
 *  - por chave válida: protege o sistema de um cliente autenticado abusivo;
 *  - por IP, só para tentativas com chave ausente ou inválida: freia força bruta.
 * A chave é validada antes, então quem divide o IP com um atacante (NAT) e tem chave
 * válida não é bloqueado pelos erros dele.
 */
class ApiKeyFilter(
    private val props: SecurityProperties,
    private val onLimited: (reason: String) -> Unit = {},
    nanoTime: () -> Long = System::nanoTime,
) : OncePerRequestFilter() {

    private val accepted: List<ByteArray> = props.apiKeys.filter { it.isNotBlank() }.map(::sha256)

    private val limits = props.rateLimit
    private val perKey = TokenBucketLimiter(
        limits.burst.toDouble(), limits.requestsPerSecond, limits.maxTrackedClients, nanoTime,
    )
    private val failedPerIp = TokenBucketLimiter(
        limits.failedAuthBurst.toDouble(), limits.failedAuthPerMinute / 60.0, limits.maxTrackedClients, nanoTime,
    )

    init {
        require(!props.enabled || accepted.isNotEmpty()) {
            "cronos.security.enabled=true, mas nenhuma chave foi configurada. " +
                    "Defina CRONOS_API_KEYS (separadas por vírgula) ou cronos.security.api-keys."
        }
    }

    override fun shouldNotFilter(request: HttpServletRequest): Boolean {
        if (!props.enabled) return true
        val path = request.requestURI.removePrefix(request.contextPath)
        return props.publicPaths.any { path == it || path.startsWith("$it/") }
    }

    override fun doFilterInternal(request: HttpServletRequest, response: HttpServletResponse, chain: FilterChain) {
        val key = extractKey(request)
        val matched = key?.let(::match)
        if (matched == null) {
            if (limits.enabled) {
                val decision = failedPerIp.tryConsume(clientIp(request))
                if (!decision.allowed) {
                    onLimited("auth")
                    return tooManyRequests(response, decision, "Muitas tentativas sem chave válida deste IP")
                }
            }
            return reject(response, if (key == null) {
                "API key ausente: envie Authorization: Bearer <chave> ou X-API-Key"
            } else "API key inválida")
        }

        if (limits.enabled) {
            val decision = perKey.tryConsume("key-$matched")
            response.setHeader("X-RateLimit-Limit", perKey.limit.toString())
            response.setHeader("X-RateLimit-Remaining", decision.remaining.toString())
            if (!decision.allowed) {
                onLimited("key")
                return tooManyRequests(response, decision, "Limite de requisições da chave excedido")
            }
        }
        chain.doFilter(request, response)
    }

    /** Índice da chave aceita, ou null. Compara com todas, sem sair no primeiro acerto (tempo constante). */
    private fun match(key: String): Int? {
        val digest = sha256(key)
        var found: Int? = null
        accepted.forEachIndexed { i, k -> if (MessageDigest.isEqual(k, digest)) found = i }
        return found
    }

    private fun extractKey(request: HttpServletRequest): String? {
        request.getHeader(HttpHeaders.AUTHORIZATION)?.let { header ->
            if (header.startsWith("Bearer ", ignoreCase = true)) {
                return header.substring(7).trim().takeIf { it.isNotEmpty() }
            }
        }
        return request.getHeader("X-API-Key")?.trim()?.takeIf { it.isNotEmpty() }
    }

    private fun clientIp(request: HttpServletRequest): String {
        if (limits.trustForwardedFor) {
            request.getHeader("X-Forwarded-For")?.split(",")?.first()?.trim()?.takeIf { it.isNotEmpty() }
                ?.let { return it }
        }
        return request.remoteAddr
    }

    private fun reject(response: HttpServletResponse, detail: String) {
        response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer")
        write(response, HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized", detail)
    }

    private fun tooManyRequests(response: HttpServletResponse, decision: TokenBucketLimiter.Decision, detail: String) {
        response.setHeader(HttpHeaders.RETRY_AFTER, decision.retryAfterSeconds.toString())
        write(response, 429, "Too Many Requests", "$detail; tente de novo em ${decision.retryAfterSeconds}s")
    }

    private fun write(response: HttpServletResponse, status: Int, title: String, detail: String) {
        response.status = status
        response.contentType = MediaType.APPLICATION_PROBLEM_JSON_VALUE
        response.characterEncoding = StandardCharsets.UTF_8.name()
        val escaped = detail.replace("\\", "\\\\").replace("\"", "\\\"")
        response.writer.write("""{"title":"$title","status":$status,"detail":"$escaped"}""")
    }

    private fun sha256(s: String): ByteArray =
        MessageDigest.getInstance("SHA-256").digest(s.toByteArray(StandardCharsets.UTF_8))
}
