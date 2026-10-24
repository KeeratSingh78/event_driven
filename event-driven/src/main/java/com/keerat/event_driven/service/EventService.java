package com.keerat.event_driven.service;

import com.keerat.event_driven.domain.Event;
import com.keerat.event_driven.domain.EventPriority;
import com.keerat.event_driven.dto.EventResponse;
import com.keerat.event_driven.dto.EventSubmissionRequest;
import com.keerat.event_driven.exception.IdempotencyConflictException;
import com.keerat.event_driven.exception.ResourceNotFoundException;
import com.keerat.event_driven.repository.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Service managing event submission, idempotency validation, and retrieval.
 */
@Service
public class EventService {

    private static final Logger log = LoggerFactory.getLogger(EventService.class);

    private final EventRepository eventRepository;
    private final EventPersistenceHelper persistenceHelper;

    public EventService(EventRepository eventRepository, EventPersistenceHelper persistenceHelper) {
        this.eventRepository = eventRepository;
        this.persistenceHelper = persistenceHelper;
    }

    /**
     * Submits an event with idempotency guarantees.
     *
     * @param rawIdempotencyKey Caller-supplied idempotency key
     * @param request           Event details (type, payload, priority)
     * @return SubmissionOutcome containing the EventResponse and whether it was newly created
     */
    public SubmissionOutcome submitEvent(String rawIdempotencyKey, EventSubmissionRequest request) {
        if (rawIdempotencyKey == null || rawIdempotencyKey.trim().isEmpty()) {
            throw new IllegalArgumentException("Idempotency-Key header must not be blank");
        }
        String idempotencyKey = rawIdempotencyKey.trim();

        // 1. Fast-path: check if event already exists
        Optional<Event> existingOpt = eventRepository.findByIdempotencyKey(idempotencyKey);
        if (existingOpt.isPresent()) {
            Event existing = existingOpt.get();
            validateSameContent(existing, request, idempotencyKey);
            log.info("Duplicate submission detected for idempotency key '{}'. Returning existing event '{}'.",
                idempotencyKey, existing.getId());
            return new SubmissionOutcome(EventResponse.from(existing), false);
        }

        // 2. Insert new event + audit log atomically
        try {
            Event created = persistenceHelper.createAndAudit(idempotencyKey, request);
            log.info("Successfully registered new event '{}' with idempotency key '{}'.",
                created.getId(), idempotencyKey);
            return new SubmissionOutcome(EventResponse.from(created), true);
        } catch (DataIntegrityViolationException ex) {
            // 3. Race condition: concurrent submission committed the same idempotency key first
            log.warn("Concurrent submission conflict for idempotency key '{}'. Resolving against stored record.",
                idempotencyKey);
            Event concurrent = eventRepository.findByIdempotencyKey(idempotencyKey)
                .orElseThrow(() -> ex);
            validateSameContent(concurrent, request, idempotencyKey);
            return new SubmissionOutcome(EventResponse.from(concurrent), false);
        }
    }

    /**
     * Retrieves an event by its unique ID.
     */
    @Transactional(readOnly = true)
    public EventResponse getEvent(UUID id) {
        Event event = eventRepository.findById(id)
            .orElseThrow(() -> new ResourceNotFoundException("Event not found with ID: " + id));
        return EventResponse.from(event);
    }

    private void validateSameContent(Event existing, EventSubmissionRequest request, String idempotencyKey) {
        EventPriority requestedPriority = request.priority() != null ? request.priority() : EventPriority.NORMAL;

        boolean sameType = Objects.equals(existing.getEventType(), request.eventType());
        boolean samePriority = existing.getPriority() == requestedPriority;
        boolean samePayload = Objects.equals(existing.getPayload(), request.payload());

        if (!sameType || !samePriority || !samePayload) {
            log.warn("Idempotency conflict for key '{}': existing [type={}, priority={}, payload={}], " +
                    "incoming [type={}, priority={}, payload={}]",
                idempotencyKey, existing.getEventType(), existing.getPriority(), existing.getPayload(),
                request.eventType(), requestedPriority, request.payload());
            throw new IdempotencyConflictException(
                "Idempotency key '%s' was already used with different request content".formatted(idempotencyKey)
            );
        }
    }

    public record SubmissionOutcome(EventResponse eventResponse, boolean newlyCreated) {
    }
}
