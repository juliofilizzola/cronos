package io.github.juliofilizzola.cronos

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration

@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {

    /**
     * Um único container para a JVM de testes inteira: classes com propriedades diferentes
     * criam contextos Spring diferentes, mas todos usam o mesmo PostgreSQL.
     */
    @Bean
    @ServiceConnection
    fun postgresContainer(): PostgreSQLContainer = POSTGRES

    companion object {
        val POSTGRES: PostgreSQLContainer = PostgreSQLContainer(DockerImageName.parse("postgres:16"))
            .withStartupTimeout(Duration.ofMinutes(3))
            .also { it.start() }
    }
}
