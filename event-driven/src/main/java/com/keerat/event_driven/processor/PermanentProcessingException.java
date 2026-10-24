package com.keerat.event_driven.processor;

/**
 * Thrown for deterministic, non-retryable business validation failures
 * (e.g. invalid payload schema, missing required fields, unsupported event type).
 */
public class PermanentProcessingException extends BusinessProcessingException {

    public PermanentProcessingException(String message) {
        super(message, false);
    }

    public PermanentProcessingException(String message, Throwable cause) {
        super(message, cause, false);
    }
}
