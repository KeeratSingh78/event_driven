package com.keerat.event_driven.processor;

/**
 * Exception thrown when simulated business processing or payload validation fails.
 */
public class BusinessProcessingException extends RuntimeException {

    private final boolean retryable;

    public BusinessProcessingException(String message) {
        this(message, false);
    }

    public BusinessProcessingException(String message, boolean retryable) {
        super(message);
        this.retryable = retryable;
    }

    public BusinessProcessingException(String message, Throwable cause) {
        this(message, cause, false);
    }

    public BusinessProcessingException(String message, Throwable cause, boolean retryable) {
        super(message, cause);
        this.retryable = retryable;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
