package io.github.juliofilizzola.cronos.config

import io.github.juliofilizzola.cronos.worker.RetryPolicy
import io.github.juliofilizzola.cronos.worker.WebhookClient
import io.github.juliofilizzola.cronos.worker.WorkerProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import tools.jackson.databind.json.JsonMapper

@Configuration
@EnableConfigurationProperties(WorkerProperties::class)
class WorkerConfig {
    @Bean fun retryPolicy(props: WorkerProperties) = RetryPolicy(props.backoffBase, props.backoffMax)
    @Bean fun webhookClient(jsonMapper: JsonMapper) = WebhookClient(jsonMapper)
}
