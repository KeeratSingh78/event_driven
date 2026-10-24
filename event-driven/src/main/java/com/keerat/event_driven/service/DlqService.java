package com.keerat.event_driven.service;

import com.keerat.event_driven.domain.Event;
import com.keerat.event_driven.domain.EventAuditLog;
import com.keerat.event_driven.domain.EventStatus;
import com.keerat.event_driven.dto.EventResponse;
import com.keerat.event_driven.exception.InvalidEventStateException;
import com.keerat.event_driven.exception.ResourceNotFoundException;
import com.keerat.event_driven.repository.EventAuditLogRepository;
import com.keerat.event_driven.repository.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Service managing Dead-Letter Queue (DLQ) inspection and manual retries.
 */
@Service
public class DlqService {

    private static final Logger log = LoggerFactory.getLogger(DlqService.class);

    private final EventRepository eventRepository;
    private final EventAuditLogRepository auditLogRepository;

    public DlqService(EventRepository eventRepository, EventAuditLogRepository auditLogRepository) {
        this.eventRepository = eventRepository;
        this.auditLogRepository = auditLogRepository;
    }

    /**
     * Retrieves a paginated list of events currently in the DLQ.
     */
    @Transactional(readOnly = true)
    public Page<EventResponse> getDlqEvents(Pageable pageable) {
        return eventRepository.findByStatus(EventStatus.DLQ, pageable)
                .map(EventResponse::from);
    }

    /**
     * Retrieves details for a specific DLQ event.
     *
     * @throws ResourceNotFoundException if the event does not exist or is not in DLQ
     */
    @Transactional(readOnly = true)
    public EventResponse getDlqEvent(UUID eventId) {
        Event event = eventRepository.findById(eventId)
                .orElseThrow(() -> new ResourceNotFoundException("Event not found with ID: " + eventId));

        if (event.getStatus() != EventStatus.DLQ) {
            throw new ResourceNotFoundException("Event " + eventId +
                    " is not in the Dead-Letter Queue (current status: " + event.getStatus() + ")");
        }

        return EventResponse.from(event);
    }

    /**
     * Atomically transitions a DLQ event back to PENDING:
     * 1. Validates that the event is currently in DLQ (rejects COMPLETED or FAILED events).
     * 2. Atomically updates status to PENDING, resets attempt count to 0, sets next_attempt_at to now.
     * 3. Prevents concurrent duplicate retries via atomic row count check.
     * 4. Logs a manual retry entry in the audit trail.
     *
     * @param eventId the event ID to retry
     * @return the updated event representation
     * @throws ResourceNotFoundException  if event does not exist
     * @throws InvalidEventStateException if event is not in DLQ or concurrent update occurs
     */
    @Transactional
    public EventResponse retryDlqEvent(UUID eventId) {
        Event event = eventRepository.findById(eventId)
                .orElseThrow(() -> new ResourceNotFoundException("Event not found with ID: " + eventId));

        if (event.getStatus() == EventStatus.COMPLETED) {
            throw new InvalidEventStateException("Event " + eventId + " is already COMPLETED and cannot be retried");
        }

        if (event.getStatus() == EventStatus.FAILED) {
            throw new InvalidEventStateException("Event " + eventId + " is in permanent FAILED state and cannot be retried via DLQ API");
        }

        if (event.getStatus() != EventStatus.DLQ) {
            throw new InvalidEventStateException("Event " + eventId + " is in status " + event.getStatus() + ", expected DLQ for retry");
        }

        Instant now = Instant.now();

        int rowsUpdated = eventRepository.atomicRequeueFromDlq(eventId, now);
        if (rowsUpdated == 0) {
            throw new InvalidEventStateException("Concurrent manual retry conflict: Event " + eventId + " was already retried or modified");
        }

        // Record manual retry audit log
        EventAuditLog auditLog = new EventAuditLog(
                UUID.randomUUID(),
                eventId,
                EventStatus.PENDING,
                (short) 0,
                "Manual retry requested from DLQ by operator; attempt counter reset",
                "dlq-operator"
        );
        auditLog.setOccurredAt(now);
        auditLogRepository.save(auditLog);

        log.info("Event {} manually requeued from DLQ to PENDING by operator", eventId);

        Event updatedEvent = eventRepository.findById(eventId).orElseThrow();
        return EventResponse.from(updatedEvent);
    }
}
