package io.github.juliofilizzola.cronos.security

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.mock.web.MockFilterChain
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.mock.web.MockHttpServletResponse
import java.net.InetAddress
import java.net.URI
import java.net.UnknownHostException

class ApiKeyFilterTest {

    private val filter = ApiKeyFilter(SecurityProperties(apiKeys = listOf("chave-a", "chave-b")))

    private fun call(path: String, headers: Map<String, String> = emptyMap()): MockHttpServletResponse {
        val request = MockHttpServletRequest("GET", path)
        headers.forEach(request::addHeader)
        val response = MockHttpServletResponse()
        filter.doFilter(request, response, MockFilterChain())
        return response
    }

    @Test
    fun `sem chave responde 401 com WWW-Authenticate`() {
        val r = call("/jobs")
        assertEquals(401, r.status)
        assertEquals("Bearer", r.getHeader("WWW-Authenticate"))
        assertTrue(r.contentAsString.contains("API key ausente"))
    }

    @Test
    fun `chave errada responde 401`() {
        assertEquals(401, call("/jobs", mapOf("Authorization" to "Bearer outra")).status)
        assertEquals(401, call("/jobs", mapOf("X-API-Key" to "chave-c")).status)
        assertEquals(401, call("/jobs", mapOf("Authorization" to "Basic chave-a")).status)
    }

    @Test
    fun `aceita qualquer chave configurada, por Bearer ou X-API-Key`() {
        assertEquals(200, call("/jobs", mapOf("Authorization" to "Bearer chave-a")).status)
        assertEquals(200, call("/jobs", mapOf("Authorization" to "bearer chave-b")).status)
        assertEquals(200, call("/jobs", mapOf("X-API-Key" to "chave-b")).status)
    }

    @Test
    fun `rotas publicas nao exigem chave, mas so o prefixo exato`() {
        assertEquals(200, call("/actuator/health").status)
        assertEquals(200, call("/actuator/health/liveness").status)
        assertEquals(401, call("/actuator/healthx").status)
        assertEquals(401, call("/actuator/prometheus").status)
    }

    @Test
    fun `habilitado sem chave nao sobe`() {
        assertThrows<IllegalArgumentException> { ApiKeyFilter(SecurityProperties(apiKeys = listOf(" "))) }
    }

    @Test
    fun `desabilitado deixa tudo passar`() {
        val open = ApiKeyFilter(SecurityProperties(enabled = false))
        val response = MockHttpServletResponse()
        open.doFilter(MockHttpServletRequest("GET", "/jobs"), response, MockFilterChain())
        assertEquals(200, response.status)
    }
}

class UrlGuardTest {

    private fun guard(
        ips: Map<String, List<String>> = emptyMap(),
        props: WebhookProperties = WebhookProperties(),
    ) = UrlGuard(props) { host ->
        val list = ips[host] ?: if (host.any { it.isLetter() } && !host.contains(':')) throw UnknownHostException(host) else listOf(host)
        list.map { InetAddress.getByName(it) }
    }

    private fun blocked(url: String, g: UrlGuard = guard()) = g.check(URI.create(url))

    @Test
    fun `bloqueia enderecos internos`() {
        listOf(
            "http://127.0.0.1/", "http://10.1.2.3/", "http://172.20.0.1/", "http://192.168.0.10/",
            "http://169.254.169.254/latest/meta-data", "http://0.0.0.0/", "http://100.64.0.1/",
            "http://[::1]/", "http://[fd00::1]/", "http://[fe80::1]/", "http://[::ffff:127.0.0.1]/",
            "http://[64:ff9b::a9fe:a9fe]/",
        ).forEach { assertNotNull(blocked(it), "$it deveria ser bloqueado") }
    }

    @Test
    fun `libera enderecos publicos`() {
        listOf("http://8.8.8.8/", "https://1.1.1.1:8443/x", "http://[2606:4700::1111]/")
            .forEach { assertNull(blocked(it), "$it deveria ser liberado") }
    }

    @Test
    fun `bloqueia host que resolve para interno, mesmo com um IP publico junto`() {
        val g = guard(mapOf("app.example.com" to listOf("93.184.216.34", "10.0.0.5")))
        val r = blocked("https://app.example.com/hook", g)!!
        assertFalse(r.retryable)
        assertTrue(r.reason.contains("10.0.0.5"))
    }

    @Test
    fun `host que nao resolve e falha temporaria`() {
        assertTrue(blocked("https://nao-existe.example/")!!.retryable)
    }

    @Test
    fun `recusa esquema, credenciais na url e porta fora da lista`() {
        assertNotNull(blocked("ftp://8.8.8.8/"))
        assertNotNull(blocked("http://user:pass@8.8.8.8/"))
        val g = guard(props = WebhookProperties(allowedPorts = listOf(443)))
        assertNotNull(blocked("http://8.8.8.8/", g))
        assertNull(blocked("https://8.8.8.8/", g))
    }

    @Test
    fun `allowedHosts e allowPrivateNetworks liberam destinos internos`() {
        val ips = mapOf("interno.svc" to listOf("10.0.0.7"))
        assertNotNull(blocked("http://interno.svc/", guard(ips)))
        assertNull(blocked("http://interno.svc/", guard(ips, WebhookProperties(allowedHosts = listOf("interno.svc")))))
        assertNull(blocked("http://127.0.0.1/", guard(props = WebhookProperties(allowPrivateNetworks = true))))
    }
}
