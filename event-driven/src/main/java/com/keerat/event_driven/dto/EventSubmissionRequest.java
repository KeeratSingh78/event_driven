package com.keerat.event_driven.dto;

import com.keerat.event_driven.domain.EventPriority;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import tools.jackson.databind.JsonNode;

/**
 * Incoming request payload for submitting a new event.
 */
public record EventSubmissionRequest(

    @NotBlank(message = "eventType is required")
    @Size(max = 100, message = "eventType must not exceed 100 characters")
    String eventType,

    @NotNull(message = "payload is required")
    JsonNode payload,

    EventPriority priority
) {
}
