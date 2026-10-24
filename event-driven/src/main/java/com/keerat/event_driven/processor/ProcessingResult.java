package com.keerat.event_driven.processor;

import tools.jackson.databind.JsonNode;

/**
 * Encapsulates the output of a successful business processing operation.
 *
 * @param resultPayload structured JSON payload detailing the outcome
 * @param resultMessage human-readable summary of the business action
 */
public record ProcessingResult(JsonNode resultPayload, String resultMessage) {
}
