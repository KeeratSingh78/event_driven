package com.keerat.event_driven.exception;

/**
 * Thrown when an idempotency key is reused with differing request content.
 */
public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException(String message) {
        super(message);
    }
}
