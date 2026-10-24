package com.keerat.event_driven.controller;

import com.keerat.event_driven.dto.EventResponse;
import com.keerat.event_driven.service.DlqService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * REST controller for Dead-Letter Queue (DLQ) operations:
 * listing failed events, retrieving failure details, and requesting manual retries.
 */
@RestController
@RequestMapping("/api/v1/dlq")
public class DlqController {

    private final DlqService dlqService;

    public DlqController(DlqService dlqService) {
        this.dlqService = dlqService;
    }

    /**
     * List DLQ events with pagination.
     */
    @GetMapping
    public ResponseEntity<Page<EventResponse>> getDlqEvents(
            @PageableDefault(size = 20, sort = "updatedAt", direction = Sort.Direction.DESC) Pageable pageable
    ) {
        Page<EventResponse> page = dlqService.getDlqEvents(pageable);
        return ResponseEntity.ok(page);
    }

    /**
     * Retrieve a specific DLQ event and its failure details.
     */
    @GetMapping("/{eventId}")
    public ResponseEntity<EventResponse> getDlqEvent(@PathVariable UUID eventId) {
        EventResponse event = dlqService.getDlqEvent(eventId);
        return ResponseEntity.ok(event);
    }

    /**
     * Manually retry an eligible DLQ event.
     */
    @PostMapping("/{eventId}/retry")
    public ResponseEntity<EventResponse> retryDlqEvent(@PathVariable UUID eventId) {
        EventResponse retriedEvent = dlqService.retryDlqEvent(eventId);
        return ResponseEntity.ok(retriedEvent);
    }
}
