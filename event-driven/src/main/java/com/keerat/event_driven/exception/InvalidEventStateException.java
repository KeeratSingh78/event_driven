package com.keerat.event_driven.exception;

/**
 * Thrown when an operation is requested on an event that is not in an eligible state.
 */
public class InvalidEventStateException extends RuntimeException {

    public InvalidEventStateException(String message) {
        super(message);
    }
}
