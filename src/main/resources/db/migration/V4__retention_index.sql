-- Usado pela limpeza de histórico (ExecutionRetention): acha rápido as execuções
-- terminadas mais antigas sem varrer as que ainda estão na fila.
CREATE INDEX idx_exec_finished ON executions (finished_at)
    WHERE status IN ('SUCCEEDED', 'FAILED', 'DEAD', 'CANCELLED');
