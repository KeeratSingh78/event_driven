-- ─────────────────────────────────────────────────────────────────────────────
-- V2__create_event_results_table.sql
--
-- Stores the outcome of a successfully processed event.
-- One row per event — the unique constraint on event_id prevents duplicate
-- result records even under concurrent workers.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE event_results (
    id              UUID        NOT NULL DEFAULT gen_random_uuid(),

    -- FK to the events table.  CASCADE DELETE keeps the DB tidy if an event
    -- is ever removed (e.g., data-retention cleanup).
    event_id        UUID        NOT NULL,

    -- Structured result data returned by the processor — stored as JSONB
    -- so each event type can return different shapes without schema changes.
    result_payload  JSONB,

    -- Human-readable summary of what the processor did.
    result_message  VARCHAR(1000),

    -- The worker / processor node that produced this result.
    processed_by    VARCHAR(255),

    -- Timestamp when the result was persisted.
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_event_results PRIMARY KEY (id),

    -- One result record per event — prevents duplicate inserts from
    -- concurrent workers or retry races.
    CONSTRAINT uq_event_results_event_id UNIQUE (event_id),

    CONSTRAINT fk_event_results_event
        FOREIGN KEY (event_id)
        REFERENCES events (id)
        ON DELETE CASCADE
);

-- ── Index ──────────────────────────────────────────────────────────────────
-- Support lookups by event_id when fetching event details
CREATE INDEX idx_event_results_event_id
    ON event_results (event_id);
