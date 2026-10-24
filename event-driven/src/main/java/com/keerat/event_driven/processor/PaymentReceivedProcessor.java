package com.keerat.event_driven.processor;

import com.keerat.event_driven.domain.Event;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.util.UUID;

/**
 * Processor for simulated PAYMENT_RECEIVED events.
 */
@Component
public class PaymentReceivedProcessor implements EventProcessor {

    public static final String EVENT_TYPE = "PAYMENT_RECEIVED";

    @Override
    public boolean supports(String eventType) {
        return EVENT_TYPE.equalsIgnoreCase(eventType);
    }

    @Override
    public ProcessingResult process(Event event) throws BusinessProcessingException {
        JsonNode payload = event.getPayload();
        if (payload == null || !payload.isObject()) {
            throw new PermanentProcessingException("PAYMENT_RECEIVED event payload must be a JSON object");
        }

        if (payload.hasNonNull("simulateFailure")) {
            String sim = payload.get("simulateFailure").asText().trim();
            if ("TRANSIENT".equalsIgnoreCase(sim) || "RETRYABLE".equalsIgnoreCase(sim)) {
                throw new RetryableProcessingException("Simulated transient payment gateway 504 Gateway Timeout");
            } else if ("PERMANENT".equalsIgnoreCase(sim)) {
                throw new PermanentProcessingException("Simulated permanent fraudulent card rejection");
            }
        }

        if (!payload.hasNonNull("paymentId") || payload.get("paymentId").asText().trim().isEmpty()) {
            throw new PermanentProcessingException("PAYMENT_RECEIVED event payload is missing required field: 'paymentId'");
        }

        if (!payload.hasNonNull("amount")) {
            throw new PermanentProcessingException("PAYMENT_RECEIVED event payload is missing required field: 'amount'");
        }

        JsonNode amountNode = payload.get("amount");
        if (!amountNode.isNumber() || amountNode.asDouble() <= 0.0) {
            throw new PermanentProcessingException("PAYMENT_RECEIVED event payload requires a positive numeric 'amount'");
        }

        String paymentId = payload.get("paymentId").asText().trim();
        double amount = amountNode.asDouble();
        String currency = payload.hasNonNull("currency") ? payload.get("currency").asText().trim() : "USD";

        ObjectNode resultNode = JsonNodeFactory.instance.objectNode();
        resultNode.put("paymentId", paymentId);
        resultNode.put("amount", amount);
        resultNode.put("currency", currency);
        resultNode.put("paymentStatus", "SETTLED");
        resultNode.put("settlementReference", "settle-" + UUID.randomUUID());

        return new ProcessingResult(resultNode, "Payment " + paymentId + " of " + amount + " " + currency + " settled successfully");
    }
}
