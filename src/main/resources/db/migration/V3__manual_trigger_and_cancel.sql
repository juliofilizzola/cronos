-- Origem da execução: SCHEDULER (criada pelo agendamento) ou MANUAL (POST /jobs/{id}/trigger).
ALTER TABLE executions
    ADD COLUMN triggered_by TEXT NOT NULL DEFAULT 'SCHEDULER';

-- Status possíveis a partir daqui:
--   PENDING | RUNNING | SUCCEEDED | FAILED | DEAD | CANCELLED
COMMENT ON COLUMN executions.status IS 'PENDING | RUNNING | SUCCEEDED | FAILED | DEAD | CANCELLED';
