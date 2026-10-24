package com.keerat.event_driven.worker.service;

import com.keerat.event_driven.domain.Event;
import com.keerat.event_driven.domain.EventAuditLog;
import com.keerat.event_driven.domain.EventStatus;
import com.keerat.event_driven.repository.EventAuditLogRepository;
import com.keerat.event_driven.repository.EventRepository;
import com.keerat.event_driven.retry.RetryPolicyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Service providing atomic recovery of events stranded in PROCESSING state
 * due to worker crashes or node failures.
 */
@Service
public class EventRecoveryService {

    private static final Logger log = LoggerFactory.getLogger(EventRecoveryService.class);

    private final EventRepository eventRepository;
    private final EventAuditLogRepository auditLogRepository;
    private final RetryPolicyService retryPolicyService;

    public EventRecoveryService(EventRepository eventRepository,
                                EventAuditLogRepository auditLogRepository,
                                RetryPolicyService retryPolicyService) {
        this.eventRepository = eventRepository;
        this.auditLogRepository = auditLogRepository;
        this.retryPolicyService = retryPolicyService;
    }

    /**
     * Atomically locks and recovers up to {@code limit} events stranded in PROCESSING
     * whose lease has expired. Invalidates the old claim token, resets the lease,
     * updates the status to PENDING (or DLQ if attempts exhausted), and logs an audit trail entry.
     *
     * @param limit maximum batch size to recover
     * @return number of events recovered
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int recoverExpiredProcessingEvents(int limit) {
        if (limit <= 0) {
            return 0;
        }

        List<Event> expiredEvents = eventRepository.findAndLockExpiredProcessingEvents(limit);
        if (expiredEvents.isEmpty()) {
            return 0;
        }

        Instant now = Instant.now();
        List<EventAuditLog> auditLogs = new ArrayList<>(expiredEvents.size());

        for (Event event : expiredEvents) {
            // Revoke current claim token and clear lease
            event.setClaimToken(null);
            event.setProcessingLeaseUntil(null);
            event.setUpdatedAt(now);

            if (retryPolicyService.isExhausted(event.getAttemptCount(), event.getMaxAttempts())) {
                String failureMessage = String.format(
                        "Processing lease expired and maximum attempts (%d) exhausted", event.getMaxAttempts());
                event.setStatus(EventStatus.DLQ);
                event.setLastFailureReason(failureMessage);

                EventAuditLog audit = new EventAuditLog(
                        UUID.randomUUID(),
                        event.getId(),
                        EventStatus.DLQ,
                        event.getAttemptCount(),
                        "Processing lease expired and max attempts exhausted; moved to DLQ by crash recovery coordinator",
                        "recovery-coordinator"
                );
                audit.setOccurredAt(now);
                auditLogs.add(audit);

                log.warn("Recovered expired event {} and moved to DLQ (attempts={})",
                        event.getId(), event.getAttemptCount());
            } else {
                event.setStatus(EventStatus.PENDING);
                event.setNextAttemptAt(now); // immediately eligible for another worker

                EventAuditLog audit = new EventAuditLog(
                        UUID.randomUUID(),
                        event.getId(),
                        EventStatus.PENDING,
                        event.getAttemptCount(),
                        "Processing lease expired; recovered and returned to PENDING by crash recovery coordinator",
                        "recovery-coordinator"
                );
                audit.setOccurredAt(now);
                auditLogs.add(audit);

                log.info("Recovered expired event {} and returned to PENDING for retry", event.getId());
            }
        }

        eventRepository.saveAll(expiredEvents);
        auditLogRepository.saveAll(auditLogs);

        return expiredEvents.size();
    }
}
