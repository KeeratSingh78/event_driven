package com.keerat.event_driven.processor;

import com.keerat.event_driven.domain.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Default implementation of {@link BusinessProcessingService} dispatching events
 * to registered {@link EventProcessor} strategies.
 */
@Service
public class DefaultBusinessProcessingService implements BusinessProcessingService {

    private static final Logger log = LoggerFactory.getLogger(DefaultBusinessProcessingService.class);

    private final List<EventProcessor> processors;

    public DefaultBusinessProcessingService(List<EventProcessor> processors) {
        this.processors = processors != null ? processors : List.of();
    }

    @Override
    public ProcessingResult process(Event event) throws BusinessProcessingException {
        if (event == null) {
            throw new PermanentProcessingException("Event must not be null");
        }

        String eventType = event.getEventType();
        if (eventType == null || eventType.trim().isEmpty()) {
            throw new PermanentProcessingException("Event type must not be null or blank");
        }

        for (EventProcessor processor : processors) {
            if (processor.supports(eventType)) {
                log.debug("Dispatching event {} of type '{}' to processor {}", event.getId(), eventType, processor.getClass().getSimpleName());
                return processor.process(event);
            }
        }

        throw new PermanentProcessingException("Unsupported event type: '" + eventType + "'");
    }
}
