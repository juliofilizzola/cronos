package io.github.juliofilizzola.cronos.persistence

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.stereotype.Repository
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

/** Execução reservada por um worker, já com o que é preciso para realizá-la. */
data class ClaimedExecution(
    val id: UUID,
    val jobId: UUID,
    val scheduledFor: Instant,
    val attempt: Int,
    val payload: String,
    val maxAttempts: Int,
    val timeoutSeconds: Int,
)

@Repository
class ExecutionRepository(private val jdbc: JdbcTemplate) {

    /**
     * Reserva até [limit] execuções vencidas. SKIP LOCKED faz cada worker pegar linhas
     * diferentes; o UPDATE no mesmo comando garante que a reserva é atômica.
     */
    fun claim(workerId: String, limit: Int, now: Instant, leaseUntil: Instant): List<ClaimedExecution> =
        jdbc.query(
            """
            WITH claimed AS (
                UPDATE executions
                SET status = 'RUNNING', locked_by = ?, lease_expires_at = ?, started_at = ?,
                    attempt = attempt + 1
                WHERE id IN (
                    SELECT id FROM executions
                    WHERE status = 'PENDING' AND run_after <= ?
                    ORDER BY run_after
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, job_id, scheduled_for, attempt
            )
            SELECT c.id, c.job_id, c.scheduled_for, c.attempt, j.payload, j.max_attempts, j.timeout_seconds
            FROM claimed c JOIN jobs j ON j.id = c.job_id
            """.trimIndent(),
            claimedMapper, workerId, utc(leaseUntil), utc(now), utc(now), limit,
        )

    /**
     * Grava o resultado. Só vale se a execução ainda for deste worker: se o lease expirou
     * e o reaper a devolveu para a fila, o resultado atrasado é descartado.
     * Retorna 1 se gravou, 0 se a execução não era mais deste worker.
     */
    fun finish(
        id: UUID,
        workerId: String,
        status: String,
        finishedAt: Instant?,
        runAfter: Instant?,
        error: String?,
    ): Int =
        jdbc.update(
            """
            UPDATE executions
            SET status = ?, finished_at = ?, run_after = COALESCE(?, run_after), last_error = ?,
                locked_by = NULL, lease_expires_at = NULL
            WHERE id = ? AND locked_by = ? AND status = 'RUNNING'
            """.trimIndent(),
            status, finishedAt?.let(::utc), runAfter?.let(::utc), error?.take(MAX_ERROR), id, workerId,
        )

    /** Heartbeat: renova o lease de tudo que este worker está executando. */
    fun extendLeases(workerId: String, leaseUntil: Instant): Int =
        jdbc.update(
            "UPDATE executions SET lease_expires_at = ? WHERE locked_by = ? AND status = 'RUNNING'",
            utc(leaseUntil), workerId,
        )

    /**
     * Reaper: execuções RUNNING com lease vencido pertencem a um worker que morreu ou travou.
     * Voltam para PENDING (ou viram DEAD se já usaram todas as tentativas).
     * Retorna quantas foram recuperadas.
     */
    fun reapExpired(now: Instant): Int =
        jdbc.update(
            """
            UPDATE executions e
            SET status = CASE WHEN e.attempt >= j.max_attempts THEN 'DEAD' ELSE 'PENDING' END,
                finished_at = CASE WHEN e.attempt >= j.max_attempts THEN ? END,
                run_after = ?,
                last_error = 'lease expirado: worker ' || COALESCE(e.locked_by, '?') || ' parou de responder',
                locked_by = NULL, lease_expires_at = NULL
            FROM jobs j
            WHERE j.id = e.job_id AND e.status = 'RUNNING' AND e.lease_expires_at < ?
            """.trimIndent(),
            utc(now), utc(now), utc(now),
        )

    private val claimedMapper = RowMapper { rs, _ ->
        ClaimedExecution(
            id = rs.getObject("id", UUID::class.java),
            jobId = rs.getObject("job_id", UUID::class.java),
            scheduledFor = rs.getObject("scheduled_for", OffsetDateTime::class.java).toInstant(),
            attempt = rs.getInt("attempt"),
            payload = rs.getString("payload"),
            maxAttempts = rs.getInt("max_attempts"),
            timeoutSeconds = rs.getInt("timeout_seconds"),
        )
    }

    private fun utc(i: Instant): OffsetDateTime = OffsetDateTime.ofInstant(i, ZoneOffset.UTC)

    private companion object {
        const val MAX_ERROR = 1000
    }
}
