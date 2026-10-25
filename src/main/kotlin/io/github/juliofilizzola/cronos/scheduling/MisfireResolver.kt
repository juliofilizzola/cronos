package io.github.juliofilizzola.cronos.scheduling

import io.github.juliofilizzola.cronos.domain.Job
import io.github.juliofilizzola.cronos.domain.MisfirePolicy
import java.time.Duration
import java.time.Instant

/**
 * @property fireAt     horários lógicos (scheduled_for) que devem virar execuções
 * @property nextFireAt novo next_fire_at do job; null = não há mais ocorrências
 * @property misfired   true se o disparo estava atrasado além da tolerância
 */
data class Resolution(
    val fireAt: List<Instant>,
    val nextFireAt: Instant?,
    val misfired: Boolean,
)

/**
 * Decide o que fazer com um job vencido (next_fire_at <= now).
 * Função pura: não acessa banco nem relógio, o que facilita os testes.
 */
class MisfireResolver(
    private val calculator: NextFireCalculator,
    private val maxCatchUp: Int = 100,
) {

    fun resolve(job: Job, now: Instant): Resolution {
        val due = requireNotNull(job.nextFireAt) { "Job ${job.id} não tem next_fire_at" }
        val lateness = Duration.between(due, now)

        // Dentro da tolerância: disparo normal, não é misfire.
        if (lateness <= job.misfireThreshold) {
            return Resolution(
                fireAt = listOf(due),
                nextFireAt = calculator.nextAfter(job, due),
                misfired = false,
            )
        }

        return when (job.misfirePolicy) {
            // Ignora o que foi perdido e segue na próxima ocorrência futura.
            MisfirePolicy.SKIP -> Resolution(
                fireAt = emptyList(),
                nextFireAt = calculator.firstAfter(job, due, now),
                misfired = true,
            )

            // Recupera com UMA execução (scheduled_for = primeiro horário perdido)
            // e pula as demais.
            MisfirePolicy.FIRE_ONCE -> Resolution(
                fireAt = listOf(due),
                nextFireAt = calculator.firstAfter(job, due, now),
                misfired = true,
            )

            MisfirePolicy.FIRE_ALL -> fireAll(job, due, now)
        }
    }

    /**
     * Gera todas as ocorrências perdidas, limitado a [maxCatchUp] por rodada
     * para uma parada longa não criar milhões de execuções de uma vez.
     * Se o limite estourar, nextFireAt aponta para a próxima ocorrência ainda
     * perdida, e o próximo tick do scheduler continua de onde parou.
     */
    private fun fireAll(job: Job, due: Instant, now: Instant): Resolution {
        val fires = mutableListOf<Instant>()
        var cursor: Instant? = due
        while (cursor != null && !cursor.isAfter(now) && fires.size < maxCatchUp) {
            fires += cursor
            cursor = calculator.nextAfter(job, cursor)
        }
        return Resolution(fireAt = fires, nextFireAt = cursor, misfired = true)
    }
}