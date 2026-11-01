package io.github.juliofilizzola.cronos.security

import org.springframework.boot.context.properties.ConfigurationProperties
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress
import java.net.URI
import java.net.UnknownHostException

@ConfigurationProperties("cronos.webhook")
data class WebhookProperties(
    /** Libera destinos em rede privada/loopback. Só para desenvolvimento e testes. */
    val allowPrivateNetworks: Boolean = false,
    /** Hosts liberados mesmo resolvendo para endereço interno (ex.: um serviço interno conhecido). */
    val allowedHosts: List<String> = emptyList(),
    /** Portas aceitas. Vazio = qualquer porta. */
    val allowedPorts: List<Int> = emptyList(),
)

/** Por que um destino foi recusado. [retryable] = true quando pode ser transitório (DNS). */
data class UrlRejection(val reason: String, val retryable: Boolean)

/**
 * Proteção contra SSRF: um job não pode fazer o worker chamar endereços internos
 * (loopback, redes privadas, link-local como o metadata da nuvem em 169.254.169.254, etc.).
 *
 * O host é resolvido e **todos** os endereços são checados: basta um interno para recusar,
 * senão um DNS com vários registros poderia misturar um IP público com um privado.
 *
 * Limitação: a checagem acontece antes da conexão, e o HttpClient resolve o nome de novo
 * ao conectar. Um DNS malicioso com TTL zero (DNS rebinding) ainda pode trocar o IP nesse
 * intervalo. Para fechar isso de vez, use um proxy de saída que aplique a mesma regra.
 */
class UrlGuard(
    private val props: WebhookProperties,
    private val resolve: (String) -> List<InetAddress> = { InetAddress.getAllByName(it).toList() },
) {

    /** Retorna null se o destino é permitido. */
    fun check(uri: URI): UrlRejection? {
        if (uri.scheme != "http" && uri.scheme != "https") return UrlRejection("url deve ser http ou https", false)
        val host = uri.host?.removePrefix("[")?.removeSuffix("]")
            ?: return UrlRejection("url sem host", false)
        if (uri.userInfo != null) return UrlRejection("url não pode conter usuário/senha", false)

        val port = if (uri.port != -1) uri.port else if (uri.scheme == "https") 443 else 80
        if (props.allowedPorts.isNotEmpty() && port !in props.allowedPorts) {
            return UrlRejection("porta $port não permitida (aceitas: ${props.allowedPorts})", false)
        }

        if (props.allowPrivateNetworks || props.allowedHosts.any { it.equals(host, ignoreCase = true) }) return null

        val addresses = try {
            resolve(host)
        } catch (e: UnknownHostException) {
            return UrlRejection("host $host não resolve", true)
        }
        val blocked = addresses.firstOrNull(::isInternal)
            ?: return null
        return UrlRejection("destino bloqueado: $host resolve para endereço interno ${blocked.hostAddress}", false)
    }

    internal companion object {
        fun isInternal(address: InetAddress): Boolean {
            if (address.isAnyLocalAddress || address.isLoopbackAddress || address.isLinkLocalAddress ||
                address.isSiteLocalAddress || address.isMulticastAddress
            ) return true
            return when (address) {
                is Inet4Address -> isInternalV4(address.address)
                is Inet6Address -> isInternalV6(address.address)
                else -> true
            }
        }

        private fun isInternalV4(b: ByteArray): Boolean {
            val o0 = b[0].toInt() and 0xff
            val o1 = b[1].toInt() and 0xff
            return o0 == 0 ||                               // 0.0.0.0/8
                    o0 == 10 ||                             // 10.0.0.0/8
                    o0 == 127 ||                            // loopback
                    (o0 == 100 && o1 in 64..127) ||         // 100.64.0.0/10 (CGNAT)
                    (o0 == 169 && o1 == 254) ||             // link-local / metadata da nuvem
                    (o0 == 172 && o1 in 16..31) ||          // 172.16.0.0/12
                    (o0 == 192 && o1 == 168) ||             // 192.168.0.0/16
                    (o0 == 192 && o1 == 0 && b[2].toInt() == 0) || // 192.0.0.0/24
                    (o0 == 198 && (o1 == 18 || o1 == 19)) ||       // 198.18.0.0/15 (benchmark)
                    o0 >= 224                               // multicast e reservado
        }

        private fun isInternalV6(b: ByteArray): Boolean {
            val first = b[0].toInt() and 0xff
            if (first and 0xfe == 0xfc) return true // fc00::/7 (ULA)
            // IPv4 embutido: ::ffff:a.b.c.d (mapeado) e ::a.b.c.d (compatível)
            val prefixZero = (0 until 10).all { b[it].toInt() == 0 }
            val mapped = prefixZero && (b[10].toInt() and 0xff) == 0xff && (b[11].toInt() and 0xff) == 0xff
            val compatible = prefixZero && b[10].toInt() == 0 && b[11].toInt() == 0
            if (mapped || compatible) return isInternalV4(b.copyOfRange(12, 16))
            // 64:ff9b::/96 (NAT64) também carrega um IPv4
            if (first == 0x00 && (b[1].toInt() and 0xff) == 0x64 && (b[2].toInt() and 0xff) == 0xff &&
                (b[3].toInt() and 0xff) == 0x9b
            ) return isInternalV4(b.copyOfRange(12, 16))
            return false
        }
    }
}
