package com.keerat.event_driven.controller;

import com.keerat.event_driven.domain.EventAuditLog;
import com.keerat.event_driven.domain.EventPriority;
import com.keerat.event_driven.domain.EventStatus;
import com.keerat.event_driven.repository.EventAuditLogRepository;
import com.keerat.event_driven.repository.EventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class EventControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private EventAuditLogRepository auditLogRepository;

    @BeforeEach
    void setUp() {
        auditLogRepository.deleteAll();
        eventRepository.deleteAll();
    }

    @Nested
    @DisplayName("POST /api/v1/events - Event Submission")
    class SubmissionTests {

        @Test
        @DisplayName("Should successfully submit a new event with generated UUID and initial audit log")
        void shouldSubmitNewEventSuccessfully() throws Exception {
            String idempotencyKey = "key-" + UUID.randomUUID();
            String requestJson = """
                {
                    "eventType": "ORDER_CREATED",
                    "payload": {
                        "orderId": "ORD-991",
                        "amount": 149.95,
                        "customer": "Keerat"
                    },
                    "priority": "HIGH"
                }
                """;

            String responseContent = mockMvc.perform(post("/api/v1/events")
                    .header("Idempotency-Key", idempotencyKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", containsString("/api/v1/events/")))
                .andExpect(jsonPath("$.id", notNullValue()))
                .andExpect(jsonPath("$.idempotencyKey", is(idempotencyKey)))
                .andExpect(jsonPath("$.eventType", is("ORDER_CREATED")))
                .andExpect(jsonPath("$.payload.orderId", is("ORD-991")))
                .andExpect(jsonPath("$.payload.amount", is(149.95)))
                .andExpect(jsonPath("$.priority", is("HIGH")))
                .andExpect(jsonPath("$.status", is("PENDING")))
                .andExpect(jsonPath("$.attemptCount", is(0)))
                .andExpect(jsonPath("$.maxAttempts", is(5)))
                .andExpect(jsonPath("$.createdAt", notNullValue()))
                .andExpect(jsonPath("$.updatedAt", notNullValue()))
                .andReturn()
                .getResponse()
                .getContentAsString();

            // Verify initial audit log entry created in database
            UUID eventId = UUID.fromString(
                com.jayway.jsonpath.JsonPath.read(responseContent, "$.id")
            );
            List<EventAuditLog> auditLogs = auditLogRepository.findByEventIdOrderByOccurredAtAsc(eventId);
            assertThat(auditLogs).hasSize(1);
            EventAuditLog initialLog = auditLogs.getFirst();
            assertThat(initialLog.getStatus()).isEqualTo(EventStatus.PENDING);
            assertThat(initialLog.getAttemptNumber()).isEqualTo((short) 0);
            assertThat(initialLog.getMessage()).isEqualTo("Event registered");
        }

        @Test
        @DisplayName("Should default priority to NORMAL when omitted from request")
        void shouldDefaultPriorityToNormalWhenOmitted() throws Exception {
            String idempotencyKey = "key-" + UUID.randomUUID();
            String requestJson = """
                {
                    "eventType": "USER_SIGNUP",
                    "payload": { "userId": 42 }
                }
                """;

            mockMvc.perform(post("/api/v1/events")
                    .header("Idempotency-Key", idempotencyKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.priority", is("NORMAL")))
                .andExpect(jsonPath("$.status", is("PENDING")));
        }

        @Test
        @DisplayName("Should return existing event (HTTP 200) for repeated key with identical content")
        void shouldReturnExistingEventForDuplicateSubmission() throws Exception {
            String idempotencyKey = "key-idempotent-" + UUID.randomUUID();
            String requestJson = """
                {
                    "eventType": "PAYMENT_PROCESSED",
                    "payload": {
                        "paymentId": "PAY-100",
                        "currency": "USD"
                    },
                    "priority": "HIGH"
                }
                """;

            // First submission -> 201 Created
            String firstResponse = mockMvc.perform(post("/api/v1/events")
                    .header("Idempotency-Key", idempotencyKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                .andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString();

            String firstEventId = com.jayway.jsonpath.JsonPath.read(firstResponse, "$.id");

            // Second submission with exact same key and content -> 200 OK with same event ID
            mockMvc.perform(post("/api/v1/events")
                    .header("Idempotency-Key", idempotencyKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is(firstEventId)))
                .andExpect(jsonPath("$.eventType", is("PAYMENT_PROCESSED")))
                .andExpect(jsonPath("$.payload.paymentId", is("PAY-100")));

            // Ensure no duplicate rows created
            assertThat(eventRepository.findAll()).hasSize(1);
            assertThat(auditLogRepository.findAll()).hasSize(1);
        }

        @Test
        @DisplayName("Should return HTTP 409 Conflict when same key is reused with different content")
        void shouldReturn409WhenKeyReusedWithDifferentPayload() throws Exception {
            String idempotencyKey = "key-conflict-" + UUID.randomUUID();
            String initialJson = """
                {
                    "eventType": "ORDER_PLACED",
                    "payload": { "orderId": "ORD-1" },
                    "priority": "NORMAL"
                }
                """;
            String conflictingJson = """
                {
                    "eventType": "ORDER_PLACED",
                    "payload": { "orderId": "ORD-DIFFERENT" },
                    "priority": "NORMAL"
                }
                """;

            // First submission succeeds
            mockMvc.perform(post("/api/v1/events")
                    .header("Idempotency-Key", idempotencyKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(initialJson))
                .andExpect(status().isCreated());

            // Conflicting submission returns 409 Conflict
            mockMvc.perform(post("/api/v1/events")
                    .header("Idempotency-Key", idempotencyKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(conflictingJson))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status", is(409)))
                .andExpect(jsonPath("$.error", is("Conflict")))
                .andExpect(jsonPath("$.message", containsString(idempotencyKey)));
        }

        @Test
        @DisplayName("Should return HTTP 409 Conflict when same key is reused with different priority")
        void shouldReturn409WhenKeyReusedWithDifferentPriority() throws Exception {
            String idempotencyKey = "key-conflict-prio-" + UUID.randomUUID();
            String initialJson = """
                {
                    "eventType": "TASK_QUEUED",
                    "payload": { "taskId": "T-1" },
                    "priority": "LOW"
                }
                """;
            String conflictingJson = """
                {
                    "eventType": "TASK_QUEUED",
                    "payload": { "taskId": "T-1" },
                    "priority": "HIGH"
                }
                """;

            mockMvc.perform(post("/api/v1/events")
                    .header("Idempotency-Key", idempotencyKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(initialJson))
                .andExpect(status().isCreated());

            mockMvc.perform(post("/api/v1/events")
                    .header("Idempotency-Key", idempotencyKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(conflictingJson))
                .andExpect(status().isConflict());
        }

        @Test
        @DisplayName("Should return HTTP 400 Bad Request when Idempotency-Key header is missing")
        void shouldRejectWhenIdempotencyKeyMissing() throws Exception {
            String requestJson = """
                {
                    "eventType": "ORDER_CREATED",
                    "payload": { "orderId": "ORD-1" }
                }
                """;

            mockMvc.perform(post("/api/v1/events")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.message", containsString("Idempotency-Key")));
        }

        @Test
        @DisplayName("Should return HTTP 400 Bad Request when Idempotency-Key is blank")
        void shouldRejectWhenIdempotencyKeyIsBlank() throws Exception {
            String requestJson = """
                {
                    "eventType": "ORDER_CREATED",
                    "payload": { "orderId": "ORD-1" }
                }
                """;

            mockMvc.perform(post("/api/v1/events")
                    .header("Idempotency-Key", "   ")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.message", containsString("Idempotency-Key header must not be blank")));
        }

        @Test
        @DisplayName("Should return HTTP 400 Bad Request when eventType is blank")
        void shouldRejectWhenEventTypeIsBlank() throws Exception {
            String requestJson = """
                {
                    "eventType": "",
                    "payload": { "key": "value" }
                }
                """;

            mockMvc.perform(post("/api/v1/events")
                    .header("Idempotency-Key", "key-valid")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.validationErrors.eventType", notNullValue()));
        }

        @Test
        @DisplayName("Should return HTTP 400 Bad Request when payload is missing")
        void shouldRejectWhenPayloadIsMissing() throws Exception {
            String requestJson = """
                {
                    "eventType": "VALID_TYPE"
                }
                """;

            mockMvc.perform(post("/api/v1/events")
                    .header("Idempotency-Key", "key-valid")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.validationErrors.payload", notNullValue()));
        }

        @Test
        @DisplayName("Should return HTTP 400 Bad Request when request body is malformed JSON")
        void shouldRejectMalformedJsonBody() throws Exception {
            mockMvc.perform(post("/api/v1/events")
                    .header("Idempotency-Key", "key-valid")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content("not a json payload {"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.message", containsString("Malformed JSON")));
        }
    }

    @Nested
    @DisplayName("GET /api/v1/events/{id} - Event Retrieval")
    class RetrievalTests {

        @Test
        @DisplayName("Should retrieve existing event by ID")
        void shouldRetrieveExistingEvent() throws Exception {
            String idempotencyKey = "key-" + UUID.randomUUID();
            String requestJson = """
                {
                    "eventType": "NOTIFICATION_SENT",
                    "payload": { "to": "user@example.com", "channel": "EMAIL" },
                    "priority": "LOW"
                }
                """;

            String postResponse = mockMvc.perform(post("/api/v1/events")
                    .header("Idempotency-Key", idempotencyKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson))
                .andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString();

            String eventId = com.jayway.jsonpath.JsonPath.read(postResponse, "$.id");

            mockMvc.perform(get("/api/v1/events/{id}", eventId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is(eventId)))
                .andExpect(jsonPath("$.eventType", is("NOTIFICATION_SENT")))
                .andExpect(jsonPath("$.priority", is("LOW")))
                .andExpect(jsonPath("$.status", is("PENDING")))
                .andExpect(jsonPath("$.attemptCount", is(0)))
                .andExpect(jsonPath("$.payload.to", is("user@example.com")))
                .andExpect(jsonPath("$.payload.channel", is("EMAIL")));
        }

        @Test
        @DisplayName("Should return HTTP 404 Not Found for unknown UUID")
        void shouldReturn404ForUnknownId() throws Exception {
            UUID unknownId = UUID.randomUUID();

            mockMvc.perform(get("/api/v1/events/{id}", unknownId))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status", is(404)))
                .andExpect(jsonPath("$.error", is("Not Found")))
                .andExpect(jsonPath("$.message", containsString(unknownId.toString())));
        }

        @Test
        @DisplayName("Should return HTTP 400 Bad Request for invalid UUID format")
        void shouldReturn400ForInvalidUuidFormat() throws Exception {
            mockMvc.perform(get("/api/v1/events/{id}", "not-a-valid-uuid"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.message", containsString("invalid format")));
        }
    }
}
