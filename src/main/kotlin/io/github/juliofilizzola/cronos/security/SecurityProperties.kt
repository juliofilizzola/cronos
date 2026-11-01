package io.github.juliofilizzola.cronos.security

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("cronos.security")
data class SecurityProperties(
    val enabled: Boolean = true,
    /**
     * Chaves aceitas. Mais de uma permite rotação: adicione a nova, migre os clientes
     * e só então remova a antiga. Em produção, passe por variável de ambiente (CRONOS_API_KEYS).
     */
    val apiKeys: List<String> = emptyList(),
    /** Rotas acessíveis sem chave (prefixos). */
    val publicPaths: List<String> = listOf("/actuator/health"),
    val rateLimit: RateLimitProperties = RateLimitProperties(),
)

data class RateLimitProperties(
    val enabled: Boolean = true,
    /** Vazão sustentada por chave de API. */
    val requestsPerSecond: Double = 200.0,
    /** Rajada tolerada por chave (tamanho do balde). */
    val burst: Int = 400,
    /** Tentativas com chave ausente ou inválida aceitas por IP, por minuto (contra força bruta). */
    val failedAuthPerMinute: Double = 10.0,
    val failedAuthBurst: Int = 10,
    /**
     * Usa o primeiro IP de X-Forwarded-For como cliente. Só ligue atrás de um proxy que
     * sobrescreve esse header; senão qualquer um troca de "IP" a cada requisição.
     */
    val trustForwardedFor: Boolean = false,
    val maxTrackedClients: Int = 10_000,
)
