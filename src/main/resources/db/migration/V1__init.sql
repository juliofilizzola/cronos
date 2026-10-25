CREATE TABLE jobs (
    id               UUID PRIMARY KEY,
    name             TEXT NOT NULL,
    schedule_type    TEXT NOT NULL,          -- CRON | INTERVAL | ONCE
    schedule_expr    TEXT NOT NULL,          -- "0 */5 * * * *", "PT5M" ou instante ISO
    timezone         TEXT NOT NULL DEFAULT 'UTC',
    payload          JSONB NOT NULL,         -- ex.: URL do webhook, headers, body
    status           TEXT NOT NULL,          -- ACTIVE | PAUSED | CANCELLED
    next_fire_at     TIMESTAMPTZ,            -- chave do scheduler
    max_attempts     INT NOT NULL DEFAULT 3,
    timeout_seconds  INT NOT NULL DEFAULT 60,
    misfire_policy   TEXT NOT NULL DEFAULT 'FIRE_ONCE', -- FIRE_ALL | FIRE_ONCE | SKIP
    allow_overlap    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_jobs_due ON jobs (next_fire_at) WHERE status = 'ACTIVE';

CREATE TABLE executions (
    id               UUID PRIMARY KEY,       -- também serve como chave de idempotência
    job_id           UUID NOT NULL REFERENCES jobs(id),
    scheduled_for    TIMESTAMPTZ NOT NULL,   -- horário lógico da execução
    status           TEXT NOT NULL,          -- PENDING | RUNNING | SUCCEEDED | FAILED | DEAD
    attempt          INT NOT NULL DEFAULT 0,
    run_after        TIMESTAMPTZ NOT NULL,   -- usado no backoff
    locked_by        TEXT,                   -- id do worker
    lease_expires_at TIMESTAMPTZ,
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    last_error       TEXT,
    UNIQUE (job_id, scheduled_for)           -- impede duplicar a mesma execução
);
CREATE INDEX idx_exec_claim ON executions (run_after) WHERE status = 'PENDING';