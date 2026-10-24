-- ─────────────────────────────────────────────────────────────────────────────
-- V3__create_event_audit_logs_table.sql
--
-- Immutable audit trail.  Every state transition, attempt, failure and
-- manual intervention is recorded here.  Rows are never updated or deleted
-- (append-only pattern).
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE event_audit_logs (
    id              UUID            NOT NULL DEFAULT gen_random_uuid(),

    -- The event this log entry belongs to.
    event_id        UUID            NOT NULL,

    -- The status the event transitioned TO when this entry was recorded.
    status          event_status    NOT NULL,

    -- Which attempt number triggered this entry (1-based).
    attempt_number  SMALLINT,

    -- Free-text description of what happened (e.g. exception message,
    -- "Moved to DLQ", "Manual retry requested by operator").
    message         TEXT,

    -- Name of the worker node / thread that wrote this entry.
    worker_id       VARCHAR(255),

    -- Timestamp of this specific event in the lifecycle.
    occurred_at     TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_event_audit_logs PRIMARY KEY (id),

    CONSTRAINT fk_audit_logs_event
        FOREIGN KEY (event_id)
        REFERENCES events (id)
        ON DELETE CASCADE
);

-- ── Indexes ────────────────────────────────────────────────────────────────
-- Fetch full audit history for a single event (GET /events/{id}/history)
CREATE INDEX idx_audit_logs_event_id_occurred
    ON event_audit_logs (event_id, occurred_at ASC);

-- Optional: find all DLQ entries across events for an operations dashboard
CREATE INDEX idx_audit_logs_status_occurred
    ON event_audit_logs (status, occurred_at DESC)
    WHERE status IN ('FAILED', 'DLQ');
