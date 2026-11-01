package io.github.juliofilizzola.cronos.config

import io.github.juliofilizzola.cronos.security.ApiKeyFilter
import io.github.juliofilizzola.cronos.security.SecurityProperties
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.web.servlet.FilterRegistrationBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.Ordered

@Configuration
@EnableConfigurationProperties(SecurityProperties::class)
class SecurityConfig {
    @Bean
    fun apiKeyFilter(props: SecurityProperties, meters: MeterRegistry) = FilterRegistrationBean(
        ApiKeyFilter(props, onLimited = { reason ->
            meters.counter("cronos.security.rate.limited", "reason", reason).increment()
        })
    ).apply {
        order = Ordered.HIGHEST_PRECEDENCE
        addUrlPatterns("/*")
    }
}
