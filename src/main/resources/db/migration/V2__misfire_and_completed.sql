-- Tolerância (em segundos) antes de um disparo atrasado ser tratado como misfire.
-- Lida pelo SchedulerLoop e usada pelo MisfireResolver.
ALTER TABLE jobs
    ADD COLUMN misfire_threshold_seconds INT NOT NULL DEFAULT 60;

-- Garante que o valor seja sempre positivo.
ALTER TABLE jobs
    ADD CONSTRAINT chk_jobs_misfire_threshold_positive
        CHECK (misfire_threshold_seconds >= 0);