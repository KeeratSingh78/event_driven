package com.keerat.event_driven.processor;

import com.keerat.event_driven.domain.Event;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

/**
 * Processor for simulated ORDER_PLACED events.
 */
@Component
public class OrderPlacedProcessor implements EventProcessor {

    public static final String EVENT_TYPE = "ORDER_PLACED";

    @Override
    public boolean supports(String eventType) {
        return EVENT_TYPE.equalsIgnoreCase(eventType);
    }

    @Override
    public ProcessingResult process(Event event) throws BusinessProcessingException {
        JsonNode payload = event.getPayload();
        if (payload == null || !payload.isObject()) {
            throw new PermanentProcessingException("ORDER_PLACED event payload must be a JSON object");
        }

        if (payload.hasNonNull("simulateFailure")) {
            String sim = payload.get("simulateFailure").asText().trim();
            if ("TRANSIENT".equalsIgnoreCase(sim) || "RETRYABLE".equalsIgnoreCase(sim)) {
                throw new RetryableProcessingException("Simulated transient fulfillment queue timeout");
            } else if ("PERMANENT".equalsIgnoreCase(sim)) {
                throw new PermanentProcessingException("Simulated permanent business rule violation");
            }
        }

        if (!payload.hasNonNull("orderId") || payload.get("orderId").asText().trim().isEmpty()) {
            throw new PermanentProcessingException("ORDER_PLACED event payload is missing required field: 'orderId'");
        }

        String orderId = payload.get("orderId").asText().trim();

        ObjectNode resultNode = JsonNodeFactory.instance.objectNode();
        resultNode.put("orderId", orderId);
        resultNode.put("orderStatus", "CONFIRMED");
        resultNode.put("fulfillmentStatus", "QUEUED_FOR_FULFILLMENT");

        if (payload.hasNonNull("customerId")) {
            resultNode.put("customerId", payload.get("customerId").asText().trim());
        }
        if (payload.hasNonNull("totalAmount")) {
            resultNode.put("totalAmount", payload.get("totalAmount").asDouble());
        }

        return new ProcessingResult(resultNode, "Order " + orderId + " processed successfully");
    }
}
