package com.keerat.event_driven.exception;

/**
 * Thrown when a worker attempts to update an event whose processing claim
 * has expired or been reassigned to another worker.
 * Extends {@link IllegalStateException} to preserve state integrity semantics.
 */
public class StaleClaimException extends IllegalStateException {

    public StaleClaimException(String message) {
        super(message);
    }
}
