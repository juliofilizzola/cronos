package io.github.juliofilizzola.cronos.scheduling

import io.github.juliofilizzola.cronos.domain.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID

class MisfireResolverTest {

    private val calculator = NextFireCalculator()
    private val resolver = MisfireResolver(calculator, maxCatchUp = 100)

    private val t0 = Instant.parse("2026-10-24T10:00:00Z")

    private fun job(
        policy: MisfirePolicy,
        type: ScheduleType = ScheduleType.INTERVAL,
        expr: String = "PT1M",
    ) = Job(
        id = UUID.randomUUID(), name = "teste", scheduleType = type, scheduleExpr = expr,
        timezone = ZoneOffset.UTC, status = JobStatus.ACTIVE, nextFireAt = t0,
        misfirePolicy = policy, misfireThreshold = Duration.ofSeconds(60), allowOverlap = false,
    )

    @Test
    fun `dentro da tolerancia dispara normalmente`() {
        val r = resolver.resolve(job(MisfirePolicy.SKIP), t0.plusSeconds(30))
        assertFalse(r.misfired)
        assertEquals(listOf(t0), r.fireAt)
        assertEquals(t0.plusSeconds(60), r.nextFireAt)
    }

    @Test
    fun `SKIP ignora o perdido e pula para o proximo futuro`() {
        val r = resolver.resolve(job(MisfirePolicy.SKIP), t0.plusSeconds(330)) // 10:05:30
        assertTrue(r.misfired)
        assertTrue(r.fireAt.isEmpty())
        assertEquals(Instant.parse("2026-10-24T10:06:00Z"), r.nextFireAt)
    }

    @Test
    fun `FIRE_ONCE cria uma execucao e pula para o proximo futuro`() {
        val r = resolver.resolve(job(MisfirePolicy.FIRE_ONCE), t0.plusSeconds(330))
        assertEquals(listOf(t0), r.fireAt)
        assertEquals(Instant.parse("2026-10-24T10:06:00Z"), r.nextFireAt)
    }

    @Test
    fun `FIRE_ALL cria todas as ocorrencias perdidas`() {
        val r = resolver.resolve(job(MisfirePolicy.FIRE_ALL), t0.plusSeconds(330))
        assertEquals(6, r.fireAt.size) // 10:00 ... 10:05
        assertEquals(Instant.parse("2026-10-24T10:06:00Z"), r.nextFireAt)
    }

    @Test
    fun `FIRE_ALL respeita o limite e continua no proximo tick`() {
        val r = resolver.resolve(job(MisfirePolicy.FIRE_ALL), t0.plus(Duration.ofHours(10)))
        assertEquals(100, r.fireAt.size)
        assertEquals(t0.plusSeconds(100 * 60L), r.nextFireAt) // ainda no passado: próximo tick segue
    }

    @Test
    fun `cron em SKIP vai para a proxima ocorrencia futura`() {
        val cron = job(MisfirePolicy.SKIP, ScheduleType.CRON, "0 0 * * * *") // toda hora cheia
        val r = resolver.resolve(cron.copy(nextFireAt = t0), t0.plus(Duration.ofHours(3)).plusSeconds(10))
        assertEquals(Instant.parse("2026-10-24T14:00:00Z"), r.nextFireAt)
    }

    @Test
    fun `ONCE perdido com FIRE_ONCE dispara e finaliza`() {
        val once = job(MisfirePolicy.FIRE_ONCE, ScheduleType.ONCE, "")
        val r = resolver.resolve(once, t0.plus(Duration.ofHours(1)))
        assertEquals(listOf(t0), r.fireAt)
        assertNull(r.nextFireAt)
    }
}