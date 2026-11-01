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
 */
class ApiKeyFilter(private val props: SecurityProperties) : OncePerRequestFilter() {

    private val accepted: List<ByteArray> = props.apiKeys.filter { it.isNotBlank() }.map(::sha256)

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
        if (key == null) return reject(response, "API key ausente: envie Authorization: Bearer <chave> ou X-API-Key")
        val digest = sha256(key)
        // Compara com todas, sem sair no primeiro acerto, pelo mesmo motivo do tempo constante.
        val ok = accepted.fold(false) { acc, k -> MessageDigest.isEqual(k, digest) or acc }
        if (!ok) return reject(response, "API key inválida")
        chain.doFilter(request, response)
    }

    private fun extractKey(request: HttpServletRequest): String? {
        request.getHeader(HttpHeaders.AUTHORIZATION)?.let { header ->
            if (header.startsWith("Bearer ", ignoreCase = true)) {
                return header.substring(7).trim().takeIf { it.isNotEmpty() }
            }
        }
        return request.getHeader("X-API-Key")?.trim()?.takeIf { it.isNotEmpty() }
    }

    private fun reject(response: HttpServletResponse, detail: String) {
        response.status = HttpServletResponse.SC_UNAUTHORIZED
        response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer")
        response.contentType = MediaType.APPLICATION_PROBLEM_JSON_VALUE
        response.characterEncoding = StandardCharsets.UTF_8.name()
        val escaped = detail.replace("\\", "\\\\").replace("\"", "\\\"")
        response.writer.write("""{"title":"Unauthorized","status":401,"detail":"$escaped"}""")
    }

    private fun sha256(s: String): ByteArray =
        MessageDigest.getInstance("SHA-256").digest(s.toByteArray(StandardCharsets.UTF_8))
}
