package io.github.juliofilizzola.cronos.persistence

import io.github.juliofilizzola.cronos.domain.ExecutionView
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

    fun findById(id: UUID): ExecutionView? =
        jdbc.query("SELECT $VIEW_COLUMNS FROM executions WHERE id = ?", viewMapper, id).firstOrNull()

    fun findByJob(jobId: UUID, limit: Int, offset: Int): List<ExecutionView> =
        jdbc.query(
            "SELECT $VIEW_COLUMNS FROM executions WHERE job_id = ? ORDER BY scheduled_for DESC LIMIT ? OFFSET ?",
            viewMapper, jobId, limit, offset,
        )

    fun hasActive(jobId: UUID): Boolean =
        jdbc.queryForObject(
            "SELECT EXISTS (SELECT 1 FROM executions WHERE job_id = ? AND status IN ('PENDING','RUNNING'))",
            Boolean::class.java, jobId,
        ) == true

    /** Dos jobs informados, quais têm execução PENDING ou RUNNING. */
    fun withActive(jobIds: Collection<UUID>): Set<UUID> =
        if (jobIds.isEmpty()) emptySet() else jdbc.queryForList(
            "SELECT DISTINCT job_id FROM executions WHERE status IN ('PENDING','RUNNING') " +
                    "AND job_id IN (${jobIds.joinToString(",") { "?" }})",
            UUID::class.java, *jobIds.toTypedArray(),
        ).filterNotNull().toSet()

    /**
     * Cria várias execuções num único batch. Retorna quantas foram criadas: as que já
     * existiam (mesmo job_id + scheduled_for) são ignoradas pelo ON CONFLICT.
     */
    fun insertAll(occurrences: List<Pair<UUID, Instant>>, runAfter: Instant, triggeredBy: String): Int {
        if (occurrences.isEmpty()) return 0
        return jdbc.batchUpdate(
            INSERT_SQL,
            occurrences.map { (jobId, scheduledFor) ->
                arrayOf<Any?>(UUID.randomUUID(), jobId, utc(scheduledFor), utc(runAfter), triggeredBy)
            },
        ).count { it == 1 }
    }

    /**
     * Cria uma execução. Retorna 1 se criou, 0 se a ocorrência (job_id, scheduled_for)
     * já existia: é a última barreira contra duplicidade.
     */
    fun insert(id: UUID, jobId: UUID, scheduledFor: Instant, runAfter: Instant, triggeredBy: String): Int =
        jdbc.update(INSERT_SQL, id, jobId, utc(scheduledFor), utc(runAfter), triggeredBy)

    /**
     * Cancela uma execução PENDING ou RUNNING. Uma RUNNING é interrompida pelo worker
     * que a detém (ele observa o status CANCELLED). locked_by fica preenchido de propósito,
     * para o worker reconhecer o que é seu.
     * Retorna 1 se cancelou, 0 se ela já estava em outro status.
     */
    fun cancel(id: UUID, now: Instant): Int =
        jdbc.update(
            """
            UPDATE executions
            SET status = 'CANCELLED', finished_at = ?, lease_expires_at = NULL,
                last_error = 'cancelada via API'
            WHERE id = ? AND status IN ('PENDING', 'RUNNING')
            """.trimIndent(),
            utc(now), id,
        )

    /**
     * Devolve para a fila uma execução DEAD ou FAILED, zerando as tentativas.
     * Mantém o mesmo id: é a mesma ocorrência, então a mesma chave de idempotência.
     */
    fun requeue(id: UUID, now: Instant): Int =
        jdbc.update(
            """
            UPDATE executions
            SET status = 'PENDING', attempt = 0, run_after = ?, finished_at = NULL,
                locked_by = NULL, lease_expires_at = NULL
            WHERE id = ? AND status IN ('DEAD', 'FAILED')
            """.trimIndent(),
            utc(now), id,
        )

    /** Das execuções informadas, quais foram canceladas enquanto este worker as executava. */
    fun findCancelled(workerId: String, ids: Collection<UUID>): List<UUID> =
        if (ids.isEmpty()) emptyList() else jdbc.queryForList(
            "SELECT id FROM executions WHERE locked_by = ? AND status = 'CANCELLED' " +
                    "AND id IN (${ids.joinToString(",") { "?" }})",
            UUID::class.java, workerId, *ids.toTypedArray(),
        ).filterNotNull()

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

    /**
     * Retenção: apaga até [limit] execuções terminadas antes de [before]. SKIP LOCKED deixa
     * várias instâncias limparem ao mesmo tempo sem disputar as mesmas linhas.
     * Retorna quantas apagou.
     */
    fun deleteFinishedBefore(before: Instant, limit: Int): Int =
        jdbc.update(
            """
            DELETE FROM executions WHERE id IN (
                SELECT id FROM executions
                WHERE status IN ('SUCCEEDED', 'FAILED', 'DEAD', 'CANCELLED') AND finished_at < ?
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            """.trimIndent(),
            utc(before), limit,
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

    private val viewMapper = RowMapper { rs, _ ->
        ExecutionView(
            id = rs.getObject("id", UUID::class.java),
            jobId = rs.getObject("job_id", UUID::class.java),
            scheduledFor = instant(rs.getObject("scheduled_for", OffsetDateTime::class.java))!!,
            status = rs.getString("status"),
            attempt = rs.getInt("attempt"),
            runAfter = instant(rs.getObject("run_after", OffsetDateTime::class.java))!!,
            startedAt = instant(rs.getObject("started_at", OffsetDateTime::class.java)),
            finishedAt = instant(rs.getObject("finished_at", OffsetDateTime::class.java)),
            lastError = rs.getString("last_error"),
            triggeredBy = rs.getString("triggered_by"),
        )
    }

    private fun utc(i: Instant): OffsetDateTime = OffsetDateTime.ofInstant(i, ZoneOffset.UTC)
    private fun instant(o: OffsetDateTime?): Instant? = o?.toInstant()

    private companion object {
        const val MAX_ERROR = 1000
        const val INSERT_SQL = """
            INSERT INTO executions (id, job_id, scheduled_for, status, run_after, triggered_by)
            VALUES (?, ?, ?, 'PENDING', ?, ?)
            ON CONFLICT (job_id, scheduled_for) DO NOTHING
        """
        const val VIEW_COLUMNS = "id, job_id, scheduled_for, status, attempt, run_after, started_at, " +
                "finished_at, last_error, triggered_by"
    }
}
