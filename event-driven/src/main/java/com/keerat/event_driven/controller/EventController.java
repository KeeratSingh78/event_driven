package com.keerat.event_driven.controller;

import com.keerat.event_driven.dto.EventResponse;
import com.keerat.event_driven.dto.EventSubmissionRequest;
import com.keerat.event_driven.service.EventService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.UUID;

/**
 * REST controller for event submission and status retrieval.
 */
@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    private final EventService eventService;

    public EventController(EventService eventService) {
        this.eventService = eventService;
    }

    /**
     * Submit an event with idempotency guarantees.
     */
    @PostMapping
    public ResponseEntity<EventResponse> submitEvent(
        @RequestHeader(name = "Idempotency-Key", required = true) String idempotencyKey,
        @Valid @RequestBody EventSubmissionRequest request,
        UriComponentsBuilder uriBuilder
    ) {
        EventService.SubmissionOutcome outcome = eventService.submitEvent(idempotencyKey, request);
        EventResponse response = outcome.eventResponse();

        if (outcome.newlyCreated()) {
            URI location = uriBuilder.path("/api/v1/events/{id}").buildAndExpand(response.id()).toUri();
            return ResponseEntity.created(location).body(response);
        } else {
            return ResponseEntity.ok(response);
        }
    }

    /**
     * Retrieve details and status of an event by its ID.
     */
    @GetMapping("/{id}")
    public ResponseEntity<EventResponse> getEvent(@PathVariable UUID id) {
        EventResponse response = eventService.getEvent(id);
        return ResponseEntity.ok(response);
    }
}
