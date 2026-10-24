package com.keerat.event_driven.processor;

import com.keerat.event_driven.domain.Event;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.util.UUID;

/**
 * Processor for simulated EMAIL_REQUESTED events.
 */
@Component
public class EmailRequestedProcessor implements EventProcessor {

    public static final String EVENT_TYPE = "EMAIL_REQUESTED";

    @Override
    public boolean supports(String eventType) {
        return EVENT_TYPE.equalsIgnoreCase(eventType);
    }

    @Override
    public ProcessingResult process(Event event) throws BusinessProcessingException {
        JsonNode payload = event.getPayload();
        if (payload == null || !payload.isObject()) {
            throw new PermanentProcessingException("EMAIL_REQUESTED event payload must be a JSON object");
        }

        if (payload.hasNonNull("simulateFailure")) {
            String sim = payload.get("simulateFailure").asText().trim();
            if ("TRANSIENT".equalsIgnoreCase(sim) || "RETRYABLE".equalsIgnoreCase(sim)) {
                throw new RetryableProcessingException("Simulated transient SMTP server connection reset");
            } else if ("PERMANENT".equalsIgnoreCase(sim)) {
                throw new PermanentProcessingException("Simulated permanent mailbox address blacklisted");
            }
        }

        if (!payload.hasNonNull("to") || payload.get("to").asText().trim().isEmpty()) {
            throw new PermanentProcessingException("EMAIL_REQUESTED event payload is missing required field: 'to'");
        }

        String to = payload.get("to").asText().trim();
        if (!to.contains("@")) {
            throw new PermanentProcessingException("EMAIL_REQUESTED event payload contains invalid email address: '" + to + "'");
        }

        ObjectNode resultNode = JsonNodeFactory.instance.objectNode();
        resultNode.put("recipient", to);
        resultNode.put("deliveryStatus", "DISPATCHED");
        resultNode.put("dispatchId", "msg-" + UUID.randomUUID());

        if (payload.hasNonNull("template")) {
            resultNode.put("template", payload.get("template").asText().trim());
        }

        return new ProcessingResult(resultNode, "Email dispatched successfully to " + to);
    }
}
