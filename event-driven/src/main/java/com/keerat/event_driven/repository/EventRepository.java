package com.keerat.event_driven.repository;

import com.keerat.event_driven.domain.Event;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface EventRepository extends JpaRepository<Event, UUID> {

    Optional<Event> findByIdempotencyKey(String idempotencyKey);

    boolean existsByIdempotencyKey(String idempotencyKey);

    @org.springframework.data.jpa.repository.Query(value = """
        SELECT * FROM events
        WHERE status = 'PENDING'
          AND (next_attempt_at IS NULL OR next_attempt_at <= now())
        ORDER BY priority DESC, created_at ASC
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    java.util.List<Event> findAndLockNextPendingEvents(@org.springframework.data.repository.query.Param("limit") int limit);

    @org.springframework.data.jpa.repository.Query(value = """
        SELECT * FROM events
        WHERE status = 'PROCESSING'
          AND processing_lease_until IS NOT NULL
          AND processing_lease_until <= now()
        ORDER BY processing_lease_until ASC
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    java.util.List<Event> findAndLockExpiredProcessingEvents(@org.springframework.data.repository.query.Param("limit") int limit);

    org.springframework.data.domain.Page<Event> findByStatus(com.keerat.event_driven.domain.EventStatus status, org.springframework.data.domain.Pageable pageable);

    java.util.Optional<Event> findByIdAndStatus(UUID id, com.keerat.event_driven.domain.EventStatus status);

    @org.springframework.data.jpa.repository.Modifying(clearAutomatically = true)
    @org.springframework.data.jpa.repository.Query(value = """
        UPDATE events
        SET status = 'PENDING'::event_status,
            attempt_count = 0,
            next_attempt_at = :now,
            claim_token = null,
            processing_lease_until = null,
            updated_at = :now
        WHERE id = :id AND status = 'DLQ'::event_status
        """, nativeQuery = true)
    int atomicRequeueFromDlq(@org.springframework.data.repository.query.Param("id") UUID id,
                             @org.springframework.data.repository.query.Param("now") java.time.Instant now);
}
