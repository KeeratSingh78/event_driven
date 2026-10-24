package com.keerat.event_driven.processor;

import com.keerat.event_driven.domain.Event;

/**
 * Service interface encapsulating business processing operations.
 */
public interface BusinessProcessingService {

    /**
     * Executes the simulated business operation for the given event.
     *
     * @param event the event to process
     * @return the processing result containing outcome details and summary message
     * @throws BusinessProcessingException if the event type is unsupported or payload validation fails
     */
    ProcessingResult process(Event event) throws BusinessProcessingException;
}
