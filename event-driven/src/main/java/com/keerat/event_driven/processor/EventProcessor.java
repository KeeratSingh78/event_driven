package com.keerat.event_driven.processor;

import com.keerat.event_driven.domain.Event;

/**
 * Strategy interface for executing business processing logic for a specific event type.
 */
public interface EventProcessor {

    /**
     * Checks if this processor supports the specified event type.
     *
     * @param eventType the event type string
     * @return true if supported, false otherwise
     */
    boolean supports(String eventType);

    /**
     * Executes the simulated business operation and returns a processing result.
     *
     * @param event the event to process
     * @return the processing result containing structured outcome and message
     * @throws BusinessProcessingException if payload validation or processing fails
     */
    ProcessingResult process(Event event) throws BusinessProcessingException;
}
