package io.github.juliofilizzola.cronos.config

import io.github.juliofilizzola.cronos.scheduling.MisfireResolver
import io.github.juliofilizzola.cronos.scheduling.NextFireCalculator
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling
import java.time.Clock

@Configuration
@EnableScheduling
class SchedulerConfig {
    @Bean fun clock(): Clock = Clock.systemUTC()
    @Bean fun nextFireCalculator() = NextFireCalculator()
    @Bean fun misfireResolver(calculator: NextFireCalculator) = MisfireResolver(calculator, maxCatchUp = 100)
}