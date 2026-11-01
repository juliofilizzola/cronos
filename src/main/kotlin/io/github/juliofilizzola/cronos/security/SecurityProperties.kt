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
)
