package com.keerat.event_driven.worker.service;

import com.keerat.event_driven.domain.Event;
import com.keerat.event_driven.domain.EventAuditLog;
import com.keerat.event_driven.domain.EventResult;
import com.keerat.event_driven.domain.EventStatus;
import com.keerat.event_driven.exception.StaleClaimException;
import com.keerat.event_driven.processor.ProcessingResult;
import com.keerat.event_driven.repository.EventAuditLogRepository;
import com.keerat.event_driven.repository.EventRepository;
import com.keerat.event_driven.repository.EventResultRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Service managing transactional completion, retry scheduling, DLQ transitions,
 * and permanent failure state transitions for claimed events.
 */
@Service
public class EventExecutionService {

    private static final Logger log = LoggerFactory.getLogger(EventExecutionService.class);

    private final EventRepository eventRepository;
    private final EventResultRepository eventResultRepository;
    private final EventAuditLogRepository auditLogRepository;

    public EventExecutionService(EventRepository eventRepository,
                                 EventResultRepository eventResultRepository,
                                 EventAuditLogRepository auditLogRepository) {
        this.eventRepository = eventRepository;
        this.eventResultRepository = eventResultRepository;
        this.auditLogRepository = auditLogRepository;
    }

    /**
     * Atomically records a successful event outcome with stale-claim fencing.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void completeEvent(UUID eventId, UUID claimToken, ProcessingResult result, String workerId) {
        Event event = eventRepository.findById(eventId)
                .orElseThrow(() -> new IllegalStateException("Cannot complete event: Event not found with ID " + eventId));

        validateClaim(event, claimToken, eventId);

        if (eventResultRepository.existsByEventId(eventId)) {
            throw new IllegalStateException("Integrity check failed: EventResult already exists for event " + eventId);
        }

        Instant now = Instant.now();

        // 1. Persist EventResult
        EventResult eventResult = new EventResult(
                UUID.randomUUID(),
                eventId,
                result.resultPayload(),
                result.resultMessage(),
                workerId
        );
        eventResult.setProcessedAt(now);
        eventResultRepository.save(eventResult);

        // 2. Transition Event to COMPLETED and clear lease
        event.setStatus(EventStatus.COMPLETED);
        event.setClaimToken(null);
        event.setProcessingLeaseUntil(null);
        event.setUpdatedAt(now);
        eventRepository.save(event);

        // 3. Persist COMPLETED audit record
        String summaryMessage = result.resultMessage() != null
                ? result.resultMessage()
                : "Event processing completed successfully by " + workerId;

        EventAuditLog auditLog = new EventAuditLog(
                UUID.randomUUID(),
                eventId,
                EventStatus.COMPLETED,
                event.getAttemptCount(),
                summaryMessage,
                workerId
        );
        auditLog.setOccurredAt(now);
        auditLogRepository.save(auditLog);

        log.info("Event {} successfully completed by worker {} on attempt {}",
                eventId, workerId, event.getAttemptCount());
    }

    /**
     * Overload for backward compatibility (Milestone 3 caller without claimToken).
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void completeEvent(UUID eventId, ProcessingResult result, String workerId) {
        completeEvent(eventId, null, result, workerId);
    }

    /**
     * Schedules an exponential backoff retry: resets status to PENDING with next_attempt_at,
     * clears worker lease, and records audit entry.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void scheduleRetry(UUID eventId, UUID claimToken, String failureReason, Instant nextAttemptAt, String workerId) {
        Event event = eventRepository.findById(eventId)
                .orElseThrow(() -> new IllegalStateException("Cannot schedule retry: Event not found with ID " + eventId));

        validateClaim(event, claimToken, eventId);

        Instant now = Instant.now();

        // Transition back to PENDING for next pickup after nextAttemptAt
        event.setStatus(EventStatus.PENDING);
        event.setNextAttemptAt(nextAttemptAt);
        event.setLastFailureReason(failureReason);
        event.setClaimToken(null);
        event.setProcessingLeaseUntil(null);
        event.setUpdatedAt(now);
        eventRepository.save(event);

        // Persist PENDING audit record noting scheduled retry
        String message = String.format("Retry scheduled for attempt %d at %s. Reason: %s",
                event.getAttemptCount() + 1, nextAttemptAt, failureReason);

        EventAuditLog auditLog = new EventAuditLog(
                UUID.randomUUID(),
                eventId,
                EventStatus.PENDING,
                event.getAttemptCount(),
                message,
                workerId
        );
        auditLog.setOccurredAt(now);
        auditLogRepository.save(auditLog);

        log.info("Event {} retry scheduled for attempt {} at {} by worker {}",
                eventId, event.getAttemptCount() + 1, nextAttemptAt, workerId);
    }

    /**
     * Moves a retry-exhausted event to the Dead-Letter Queue (DLQ).
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void moveToDlq(UUID eventId, UUID claimToken, String failureReason, String workerId) {
        Event event = eventRepository.findById(eventId)
                .orElseThrow(() -> new IllegalStateException("Cannot move to DLQ: Event not found with ID " + eventId));

        validateClaim(event, claimToken, eventId);

        Instant now = Instant.now();

        String terminalReason = String.format("Max attempts (%d) exhausted: %s",
                event.getMaxAttempts(), failureReason != null ? failureReason : "Unknown error");

        // Transition to DLQ
        event.setStatus(EventStatus.DLQ);
        event.setLastFailureReason(terminalReason);
        event.setClaimToken(null);
        event.setProcessingLeaseUntil(null);
        event.setUpdatedAt(now);
        eventRepository.save(event);

        // Persist DLQ audit record
        EventAuditLog auditLog = new EventAuditLog(
                UUID.randomUUID(),
                eventId,
                EventStatus.DLQ,
                event.getAttemptCount(),
                terminalReason,
                workerId
        );
        auditLog.setOccurredAt(now);
        auditLogRepository.save(auditLog);

        log.warn("Event {} exhausted max attempts ({}); moved to DLQ by worker {}",
                eventId, event.getMaxAttempts(), workerId);
    }

    /**
     * Records a permanent (non-retryable) failure: transitions status to FAILED.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordPermanentFailure(UUID eventId, UUID claimToken, String failureReason, String workerId) {
        Event event = eventRepository.findById(eventId)
                .orElseThrow(() -> new IllegalStateException("Cannot record failure: Event not found with ID " + eventId));

        validateClaim(event, claimToken, eventId);

        Instant now = Instant.now();

        // Transition to FAILED and clear lease
        event.setStatus(EventStatus.FAILED);
        event.setLastFailureReason(failureReason);
        event.setClaimToken(null);
        event.setProcessingLeaseUntil(null);
        event.setUpdatedAt(now);
        eventRepository.save(event);

        // Persist FAILED audit record
        EventAuditLog auditLog = new EventAuditLog(
                UUID.randomUUID(),
                eventId,
                EventStatus.FAILED,
                event.getAttemptCount(),
                failureReason != null ? failureReason : "Processing failed",
                workerId
        );
        auditLog.setOccurredAt(now);
        auditLogRepository.save(auditLog);

        log.warn("Event {} permanently failed by worker {} on attempt {}: {}",
                eventId, workerId, event.getAttemptCount(), failureReason);
    }

    /**
     * Backward-compatible overload for Milestone 3 calls.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void failEvent(UUID eventId, String failureReason, String workerId) {
        recordPermanentFailure(eventId, null, failureReason, workerId);
    }

    private void validateClaim(Event event, UUID claimToken, UUID eventId) {
        if (event.getStatus() != EventStatus.PROCESSING) {
            throw new StaleClaimException("Integrity check failed: Event " + eventId +
                    " status is " + event.getStatus() + ", expected " + EventStatus.PROCESSING);
        }

        if (claimToken != null && !Objects.equals(event.getClaimToken(), claimToken)) {
            throw new StaleClaimException("Stale claim: Event " + eventId +
                    " claim token mismatch (claim expired or reassigned)");
        }
    }
}
