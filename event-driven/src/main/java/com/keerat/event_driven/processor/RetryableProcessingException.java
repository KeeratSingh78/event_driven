package com.keerat.event_driven.processor;

/**
 * Thrown for transient failures (e.g. simulated downstream timeouts, temporary
 * service unreachability, transient locking conflicts) that are eligible for retry.
 */
public class RetryableProcessingException extends BusinessProcessingException {

    public RetryableProcessingException(String message) {
        super(message, true);
    }

    public RetryableProcessingException(String message, Throwable cause) {
        super(message, cause, true);
    }
}
