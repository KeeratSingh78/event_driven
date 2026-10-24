package com.keerat.event_driven.dto;

import com.keerat.event_driven.domain.Event;
import com.keerat.event_driven.domain.EventPriority;
import com.keerat.event_driven.domain.EventStatus;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

/**
 * Public representation of an event returned by the REST APIs.
 */
public record EventResponse(
    UUID id,
    String idempotencyKey,
    String eventType,
    JsonNode payload,
    EventPriority priority,
    EventStatus status,
    short attemptCount,
    short maxAttempts,
    Instant nextAttemptAt,
    String lastFailureReason,
    Instant createdAt,
    Instant updatedAt
) {
    public static EventResponse from(Event event) {
        return new EventResponse(
            event.getId(),
            event.getIdempotencyKey(),
            event.getEventType(),
            event.getPayload(),
            event.getPriority(),
            event.getStatus(),
            event.getAttemptCount(),
            event.getMaxAttempts(),
            event.getNextAttemptAt(),
            event.getLastFailureReason(),
            event.getCreatedAt(),
            event.getUpdatedAt()
        );
    }
}
