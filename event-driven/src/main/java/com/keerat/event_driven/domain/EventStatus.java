package com.keerat.event_driven.domain;

/**
 * Lifecycle states of an event within the processing pipeline.
 */
public enum EventStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    FAILED,
    DLQ
}
