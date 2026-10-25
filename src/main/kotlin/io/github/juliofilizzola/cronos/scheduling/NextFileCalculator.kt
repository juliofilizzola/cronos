package io.github.juliofilizzola.cronos.scheduling

import io.github.juliofilizzola.cronos.domain.Job
import io.github.juliofilizzola.cronos.domain.ScheduleType
import org.springframework.scheduling.support.CronExpression
import java.time.Duration
import java.time.Instant

/**
 * Calcula ocorrências de um job. Cron é avaliado com ZonedDateTime,
 * então fuso e horário de verão são tratados pelo próprio java.time.
 */
class NextFireCalculator {

    /** Próxima ocorrência estritamente depois de [reference]. Null se não houver mais. */
    fun nextAfter(job: Job, reference: Instant): Instant? = when (job.scheduleType) {
        ScheduleType.CRON ->
            CronExpression.parse(job.scheduleExpr)
                .next(reference.atZone(job.timezone))
                ?.toInstant()
        ScheduleType.INTERVAL -> reference.plus(interval(job))
        ScheduleType.ONCE -> null
    }

    /**
     * Primeira ocorrência estritamente depois de [now], seguindo a série que
     * contém [scheduled]. Evita iterar milhares de vezes após uma longa parada.
     */
    fun firstAfter(job: Job, scheduled: Instant, now: Instant): Instant? = when (job.scheduleType) {
        ScheduleType.CRON ->
            // cron é "sem memória": a próxima ocorrência depois de agora basta
            nextAfter(job, now)
        ScheduleType.INTERVAL -> {
            val interval = interval(job)
            if (scheduled.isAfter(now)) {
                scheduled
            } else {
                val steps = Duration.between(scheduled, now).dividedBy(interval) + 1
                scheduled.plus(interval.multipliedBy(steps))
            }
        }
        ScheduleType.ONCE -> null
    }

    private fun interval(job: Job): Duration =
        Duration.parse(job.scheduleExpr).also {
            require(!it.isZero && !it.isNegative) { "Intervalo deve ser positivo: ${job.scheduleExpr}" }
        }
}