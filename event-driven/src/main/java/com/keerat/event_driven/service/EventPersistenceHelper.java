package com.keerat.event_driven.service;

import com.keerat.event_driven.domain.Event;
import com.keerat.event_driven.domain.EventAuditLog;
import com.keerat.event_driven.domain.EventPriority;
import com.keerat.event_driven.domain.EventStatus;
import com.keerat.event_driven.dto.EventSubmissionRequest;
import com.keerat.event_driven.repository.EventAuditLogRepository;
import com.keerat.event_driven.repository.EventRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Isolated persistence boundary for atomic event registration and initial audit logging.
 * Runs in REQUIRES_NEW so unique constraint violations under concurrency do not poison
 * the caller's persistence context.
 */
@Component
public class EventPersistenceHelper {

    private final EventRepository eventRepository;
    private final EventAuditLogRepository eventAuditLogRepository;
    private final com.keerat.event_driven.config.RetryProperties retryProperties;

    public EventPersistenceHelper(EventRepository eventRepository,
                                  EventAuditLogRepository eventAuditLogRepository,
                                  com.keerat.event_driven.config.RetryProperties retryProperties) {
        this.eventRepository = eventRepository;
        this.eventAuditLogRepository = eventAuditLogRepository;
        this.retryProperties = retryProperties;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Event createAndAudit(String idempotencyKey, EventSubmissionRequest request) {
        UUID eventId = UUID.randomUUID();
        EventPriority priority = request.priority() != null ? request.priority() : EventPriority.NORMAL;

        Event event = new Event(
            eventId,
            idempotencyKey,
            request.eventType(),
            request.payload(),
            priority
        );
        event.setMaxAttempts((short) retryProperties.getDefaultMaxAttempts());
        Event savedEvent = eventRepository.save(event);

        EventAuditLog auditLog = new EventAuditLog(
            UUID.randomUUID(),
            savedEvent.getId(),
            EventStatus.PENDING,
            (short) 0,
            "Event registered",
            "api"
        );
        eventAuditLogRepository.save(auditLog);

        return savedEvent;
    }
}
