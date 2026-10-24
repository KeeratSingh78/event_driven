-- ─────────────────────────────────────────────────────────────────────────────
-- V4__add_event_processing_recovery.sql
--
-- Adds processing lease and claim fencing token for worker crash recovery
-- and stale-claim protection.
-- ─────────────────────────────────────────────────────────────────────────────

ALTER TABLE events
    ADD COLUMN claim_token UUID,
    ADD COLUMN processing_lease_until TIMESTAMPTZ;

-- Index to quickly find expired claims during crash recovery
CREATE INDEX idx_events_processing_lease
    ON events (status, processing_lease_until)
    WHERE status = 'PROCESSING';

-- Index to support paginated queries on the Dead-Letter Queue
CREATE INDEX idx_events_dlq
    ON events (status, updated_at DESC)
    WHERE status = 'DLQ';
