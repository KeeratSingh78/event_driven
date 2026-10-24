package com.keerat.event_driven.worker.service;

import com.keerat.event_driven.domain.Event;
import com.keerat.event_driven.domain.EventAuditLog;
import com.keerat.event_driven.domain.EventStatus;
import com.keerat.event_driven.repository.EventAuditLogRepository;
import com.keerat.event_driven.repository.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Service responsible for atomically claiming eligible PENDING events using
 * PostgreSQL FOR UPDATE SKIP LOCKED.
 *
 * <p>Each claim operation runs in its own short-lived database transaction
 * ({@link Propagation#REQUIRES_NEW}) that updates status to PROCESSING, increments
 * attempt count, records an audit log entry, and commits immediately, releasing row locks
 * before business processing begins.
 */
@Service
public class EventClaimService {

    private static final Logger log = LoggerFactory.getLogger(EventClaimService.class);

    private final EventRepository eventRepository;
    private final EventAuditLogRepository auditLogRepository;
    private final com.keerat.event_driven.config.RetryProperties retryProperties;

    public EventClaimService(EventRepository eventRepository,
                             EventAuditLogRepository auditLogRepository,
                             com.keerat.event_driven.config.RetryProperties retryProperties) {
        this.eventRepository = eventRepository;
        this.auditLogRepository = auditLogRepository;
        this.retryProperties = retryProperties;
    }

    /**
     * Atomically selects and locks up to {@code limit} eligible PENDING events in priority order
     * (HIGH > NORMAL > LOW, oldest created_at first), transitions them to PROCESSING,
     * increments the attempt count, writes an audit record for each, and commits the transaction.
     *
     * @param workerId the identifier of the worker claiming the events
     * @param limit    the maximum number of events to claim
     * @return the list of claimed events with updated state
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<Event> claimEvents(String workerId, int limit) {
        if (limit <= 0) {
            return Collections.emptyList();
        }

        List<Event> claimedEvents = eventRepository.findAndLockNextPendingEvents(limit);
        if (claimedEvents.isEmpty()) {
            return Collections.emptyList();
        }

        Instant now = Instant.now();
        Instant leaseUntil = now.plus(retryProperties.getLeaseDuration());
        List<EventAuditLog> auditLogs = new ArrayList<>(claimedEvents.size());

        for (Event event : claimedEvents) {
            short newAttempt = (short) (event.getAttemptCount() + 1);
            UUID claimToken = UUID.randomUUID();
            event.setStatus(EventStatus.PROCESSING);
            event.setAttemptCount(newAttempt);
            event.setClaimToken(claimToken);
            event.setProcessingLeaseUntil(leaseUntil);
            event.setUpdatedAt(now);

            EventAuditLog audit = new EventAuditLog(
                    UUID.randomUUID(),
                    event.getId(),
                    EventStatus.PROCESSING,
                    newAttempt,
                    "Event claimed for processing by worker " + workerId,
                    workerId
            );
            audit.setOccurredAt(now);
            auditLogs.add(audit);

            log.info("Worker {} claimed event {} (priority={}, attempt={}, leaseUntil={})",
                    workerId, event.getId(), event.getPriority(), newAttempt, leaseUntil);
        }

        eventRepository.saveAll(claimedEvents);
        auditLogRepository.saveAll(auditLogs);

        return claimedEvents;
    }
}
