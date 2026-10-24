package com.keerat.event_driven.worker;

import com.keerat.event_driven.config.WorkerProperties;
import com.keerat.event_driven.domain.Event;
import com.keerat.event_driven.domain.EventAuditLog;
import com.keerat.event_driven.domain.EventPriority;
import com.keerat.event_driven.domain.EventResult;
import com.keerat.event_driven.domain.EventStatus;
import com.keerat.event_driven.dto.EventSubmissionRequest;
import com.keerat.event_driven.processor.ProcessingResult;
import com.keerat.event_driven.repository.EventAuditLogRepository;
import com.keerat.event_driven.repository.EventRepository;
import com.keerat.event_driven.repository.EventResultRepository;
import com.keerat.event_driven.service.EventPersistenceHelper;
import com.keerat.event_driven.worker.service.EventClaimService;
import com.keerat.event_driven.worker.service.EventExecutionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@ActiveProfiles("test")
class EventProcessingIntegrationTest {

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private EventResultRepository eventResultRepository;

    @Autowired
    private EventAuditLogRepository auditLogRepository;

    @Autowired
    private EventPersistenceHelper persistenceHelper;

    @Autowired
    private EventClaimService claimService;

    @Autowired
    private EventExecutionService executionService;

    @Autowired
    private EventWorkerManager workerManager;

    @Autowired
    private WorkerProperties workerProperties;

    @BeforeEach
    void setUp() {
        eventResultRepository.deleteAll();
        auditLogRepository.deleteAll();
        eventRepository.deleteAll();
    }

    @Test
    @DisplayName("1 & 2. Valid PENDING event becomes COMPLETED with result and full audit history")
    void shouldProcessValidEventToCompletedWithResultAndAuditHistory() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("orderId", "ORD-99901");
        payload.put("customerId", "CUST-42");
        payload.put("totalAmount", 149.50);

        Event event = persistenceHelper.createAndAudit(
                "idemp-order-1",
                new EventSubmissionRequest("ORDER_PLACED", payload, EventPriority.HIGH)
        );

        assertThat(event.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(event.getAttemptCount()).isEqualTo((short) 0);

        // Process batch with worker-1
        int processed = workerManager.processNextBatch("worker-1", 5);
        assertThat(processed).isEqualTo(1);

        // 1. Verify Event state
        Event completedEvent = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(completedEvent.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(completedEvent.getAttemptCount()).isEqualTo((short) 1);
        assertThat(completedEvent.getLastFailureReason()).isNull();

        // 2. Verify EventResult
        EventResult result = eventResultRepository.findByEventId(event.getId()).orElseThrow();
        assertThat(result.getProcessedBy()).isEqualTo("worker-1");
        assertThat(result.getResultMessage()).contains("ORD-99901");
        assertThat(result.getResultPayload()).isNotNull();
        assertThat(result.getResultPayload().get("orderStatus").asText()).isEqualTo("CONFIRMED");
        assertThat(result.getResultPayload().get("fulfillmentStatus").asText()).isEqualTo("QUEUED_FOR_FULFILLMENT");

        // 3. Verify Audit History
        List<EventAuditLog> auditLogs = auditLogRepository.findByEventIdOrderByOccurredAtAsc(event.getId());
        assertThat(auditLogs).hasSize(3);

        // Initial submission
        assertThat(auditLogs.get(0).getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(auditLogs.get(0).getWorkerId()).isEqualTo("api");

        // Worker claim
        assertThat(auditLogs.get(1).getStatus()).isEqualTo(EventStatus.PROCESSING);
        assertThat(auditLogs.get(1).getAttemptNumber()).isEqualTo((short) 1);
        assertThat(auditLogs.get(1).getWorkerId()).isEqualTo("worker-1");

        // Worker completion
        assertThat(auditLogs.get(2).getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(auditLogs.get(2).getAttemptNumber()).isEqualTo((short) 1);
        assertThat(auditLogs.get(2).getWorkerId()).isEqualTo("worker-1");
    }

    @Test
    @DisplayName("Simulated business operations for EMAIL_REQUESTED and PAYMENT_RECEIVED")
    void shouldProcessEmailAndPaymentEventsSuccessfully() {
        // Email event
        ObjectNode emailPayload = JsonNodeFactory.instance.objectNode();
        emailPayload.put("to", "customer@example.com");
        emailPayload.put("template", "WELCOME_EMAIL");
        Event emailEvent = persistenceHelper.createAndAudit(
                "idemp-email-1",
                new EventSubmissionRequest("EMAIL_REQUESTED", emailPayload, EventPriority.NORMAL)
        );

        // Payment event
        ObjectNode paymentPayload = JsonNodeFactory.instance.objectNode();
        paymentPayload.put("paymentId", "PAY-888");
        paymentPayload.put("amount", 250.00);
        paymentPayload.put("currency", "EUR");
        Event paymentEvent = persistenceHelper.createAndAudit(
                "idemp-pay-1",
                new EventSubmissionRequest("PAYMENT_RECEIVED", paymentPayload, EventPriority.NORMAL)
        );

        int processed = workerManager.processNextBatch("worker-1", 10);
        assertThat(processed).isEqualTo(2);

        // Verify Email Result
        EventResult emailResult = eventResultRepository.findByEventId(emailEvent.getId()).orElseThrow();
        assertThat(emailResult.getResultPayload().get("recipient").asText()).isEqualTo("customer@example.com");
        assertThat(emailResult.getResultPayload().get("deliveryStatus").asText()).isEqualTo("DISPATCHED");

        // Verify Payment Result
        EventResult payResult = eventResultRepository.findByEventId(paymentEvent.getId()).orElseThrow();
        assertThat(payResult.getResultPayload().get("paymentId").asText()).isEqualTo("PAY-888");
        assertThat(payResult.getResultPayload().get("amount").asDouble()).isEqualTo(250.00);
        assertThat(payResult.getResultPayload().get("paymentStatus").asText()).isEqualTo("SETTLED");
    }

    @Test
    @DisplayName("3. HIGH priority events are preferred over NORMAL and LOW events, and older events first")
    void shouldPrioritizeHighOverNormalAndLow() throws Exception {
        ObjectNode dummyPayload = JsonNodeFactory.instance.objectNode();
        dummyPayload.put("orderId", "ORD-PRIORITY");

        // Insert LOW first
        Event lowEvent = persistenceHelper.createAndAudit(
                "key-low",
                new EventSubmissionRequest("ORDER_PLACED", dummyPayload, EventPriority.LOW)
        );
        Thread.sleep(20);

        // Insert NORMAL second
        Event normalEvent = persistenceHelper.createAndAudit(
                "key-norm",
                new EventSubmissionRequest("ORDER_PLACED", dummyPayload, EventPriority.NORMAL)
        );
        Thread.sleep(20);

        // Insert older HIGH event
        Event highEventOlder = persistenceHelper.createAndAudit(
                "key-high-old",
                new EventSubmissionRequest("ORDER_PLACED", dummyPayload, EventPriority.HIGH)
        );
        Thread.sleep(20);

        // Insert newer HIGH event
        Event highEventNewer = persistenceHelper.createAndAudit(
                "key-high-new",
                new EventSubmissionRequest("ORDER_PLACED", dummyPayload, EventPriority.HIGH)
        );

        // Claim 1: should claim older HIGH
        List<Event> claim1 = claimService.claimEvents("worker-test", 1);
        assertThat(claim1).hasSize(1);
        assertThat(claim1.get(0).getId()).isEqualTo(highEventOlder.getId());
        assertThat(claim1.get(0).getPriority()).isEqualTo(EventPriority.HIGH);

        // Claim 2: should claim newer HIGH
        List<Event> claim2 = claimService.claimEvents("worker-test", 1);
        assertThat(claim2).hasSize(1);
        assertThat(claim2.get(0).getId()).isEqualTo(highEventNewer.getId());
        assertThat(claim2.get(0).getPriority()).isEqualTo(EventPriority.HIGH);

        // Claim 3: should claim NORMAL
        List<Event> claim3 = claimService.claimEvents("worker-test", 1);
        assertThat(claim3).hasSize(1);
        assertThat(claim3.get(0).getId()).isEqualTo(normalEvent.getId());
        assertThat(claim3.get(0).getPriority()).isEqualTo(EventPriority.NORMAL);

        // Claim 4: should claim LOW
        List<Event> claim4 = claimService.claimEvents("worker-test", 1);
        assertThat(claim4).hasSize(1);
        assertThat(claim4.get(0).getId()).isEqualTo(lowEvent.getId());
        assertThat(claim4.get(0).getPriority()).isEqualTo(EventPriority.LOW);
    }

    @Test
    @DisplayName("4. Multiple concurrent workers claim different events without duplicate claims")
    void shouldClaimDifferentEventsConcurrentlyWithoutDuplicates() throws Exception {
        int totalEvents = 12;
        int workerCount = 3;
        int batchPerWorker = 4;

        // Seed 12 events
        for (int i = 1; i <= totalEvents; i++) {
            ObjectNode payload = JsonNodeFactory.instance.objectNode();
            payload.put("orderId", "ORD-CONC-" + i);
            persistenceHelper.createAndAudit(
                    "key-conc-" + i,
                    new EventSubmissionRequest("ORDER_PLACED", payload, EventPriority.NORMAL)
            );
        }

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(workerCount);
        Set<UUID> allClaimedEventIds = ConcurrentHashMap.newKeySet();
        List<Throwable> threadErrors = Collections.synchronizedList(new ArrayList<>());

        ExecutorService executor = Executors.newFixedThreadPool(workerCount);

        for (int w = 1; w <= workerCount; w++) {
            String workerId = "worker-" + w;
            executor.submit(() -> {
                try {
                    startLatch.await(); // ensure simultaneous execution
                    List<Event> claimed = claimService.claimEvents(workerId, batchPerWorker);
                    for (Event e : claimed) {
                        allClaimedEventIds.add(e.getId());
                    }
                } catch (Throwable t) {
                    threadErrors.add(t);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Fire all threads simultaneously
        startLatch.countDown();
        boolean completed = doneLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(threadErrors).isEmpty();

        // Exactly totalEvents claimed, with zero duplicates
        assertThat(allClaimedEventIds).hasSize(totalEvents);

        // Verify all events in database are now PROCESSING with attempt_count = 1
        List<Event> allInDb = eventRepository.findAll();
        assertThat(allInDb).hasSize(totalEvents);
        for (Event e : allInDb) {
            assertThat(e.getStatus()).isEqualTo(EventStatus.PROCESSING);
            assertThat(e.getAttemptCount()).isEqualTo((short) 1);
        }

        // Verify audit logs show PROCESSING for each event with the respective worker
        for (Event e : allInDb) {
            List<EventAuditLog> logs = auditLogRepository.findByEventIdOrderByOccurredAtAsc(e.getId());
            assertThat(logs).hasSize(2); // PENDING (api) -> PROCESSING (worker-X)
            assertThat(logs.get(1).getStatus()).isEqualTo(EventStatus.PROCESSING);
            assertThat(logs.get(1).getWorkerId()).startsWith("worker-");
        }
    }

    @Test
    @DisplayName("5. Unsupported event type transitions to FAILED with a useful failure reason")
    void shouldFailUnsupportedEventTypeWithReason() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("foo", "bar");

        Event event = persistenceHelper.createAndAudit(
                "key-unsupported",
                new EventSubmissionRequest("UNSUPPORTED_TYPE_XYZ", payload, EventPriority.NORMAL)
        );

        int processed = workerManager.processNextBatch("worker-1", 5);
        assertThat(processed).isEqualTo(1);

        Event failedEvent = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(failedEvent.getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(failedEvent.getLastFailureReason()).contains("Unsupported event type: 'UNSUPPORTED_TYPE_XYZ'");

        // Result record must NOT exist
        assertThat(eventResultRepository.findByEventId(event.getId())).isEmpty();

        // Audit log must reflect FAILED status with failure message
        List<EventAuditLog> logs = auditLogRepository.findByEventIdOrderByOccurredAtAsc(event.getId());
        assertThat(logs).hasSize(3); // PENDING -> PROCESSING -> FAILED
        EventAuditLog failLog = logs.get(2);
        assertThat(failLog.getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(failLog.getMessage()).contains("Unsupported event type");
        assertThat(failLog.getWorkerId()).isEqualTo("worker-1");
    }

    @Test
    @DisplayName("5b. Invalid payload for supported type transitions to FAILED with validation reason")
    void shouldFailInvalidPayloadWithValidationReason() {
        // Missing required orderId in ORDER_PLACED
        ObjectNode emptyPayload = JsonNodeFactory.instance.objectNode();
        Event invalidOrder = persistenceHelper.createAndAudit(
                "key-invalid-order",
                new EventSubmissionRequest("ORDER_PLACED", emptyPayload, EventPriority.NORMAL)
        );

        // Missing/negative amount in PAYMENT_RECEIVED
        ObjectNode badPaymentPayload = JsonNodeFactory.instance.objectNode();
        badPaymentPayload.put("paymentId", "PAY-INVALID");
        badPaymentPayload.put("amount", -50.0);
        Event invalidPayment = persistenceHelper.createAndAudit(
                "key-invalid-payment",
                new EventSubmissionRequest("PAYMENT_RECEIVED", badPaymentPayload, EventPriority.NORMAL)
        );

        workerManager.processNextBatch("worker-1", 5);

        Event failedOrder = eventRepository.findById(invalidOrder.getId()).orElseThrow();
        assertThat(failedOrder.getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(failedOrder.getLastFailureReason()).contains("missing required field: 'orderId'");

        Event failedPayment = eventRepository.findById(invalidPayment.getId()).orElseThrow();
        assertThat(failedPayment.getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(failedPayment.getLastFailureReason()).contains("positive numeric 'amount'");
    }

    @Test
    @DisplayName("6. A failure in one event does not stop processing other events")
    void shouldContinueProcessingWhenOneEventFails() {
        // Event 1: will fail (unsupported type)
        ObjectNode badPayload = JsonNodeFactory.instance.objectNode();
        Event badEvent = persistenceHelper.createAndAudit(
                "key-bad-1",
                new EventSubmissionRequest("UNKNOWN_BATCH_OP", badPayload, EventPriority.HIGH)
        );

        // Event 2: will succeed (valid ORDER_PLACED)
        ObjectNode orderPayload = JsonNodeFactory.instance.objectNode();
        orderPayload.put("orderId", "ORD-SUCCESS-1");
        Event orderEvent = persistenceHelper.createAndAudit(
                "key-good-order",
                new EventSubmissionRequest("ORDER_PLACED", orderPayload, EventPriority.HIGH)
        );

        // Event 3: will succeed (valid EMAIL_REQUESTED)
        ObjectNode emailPayload = JsonNodeFactory.instance.objectNode();
        emailPayload.put("to", "user@domain.com");
        Event emailEvent = persistenceHelper.createAndAudit(
                "key-good-email",
                new EventSubmissionRequest("EMAIL_REQUESTED", emailPayload, EventPriority.HIGH)
        );

        // Process batch of 5
        int processed = workerManager.processNextBatch("worker-1", 5);
        assertThat(processed).isEqualTo(3);

        // Event 1 is FAILED
        Event reloadedBad = eventRepository.findById(badEvent.getId()).orElseThrow();
        assertThat(reloadedBad.getStatus()).isEqualTo(EventStatus.FAILED);

        // Event 2 is COMPLETED with result
        Event reloadedOrder = eventRepository.findById(orderEvent.getId()).orElseThrow();
        assertThat(reloadedOrder.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(eventResultRepository.findByEventId(orderEvent.getId())).isPresent();

        // Event 3 is COMPLETED with result
        Event reloadedEmail = eventRepository.findById(emailEvent.getId()).orElseThrow();
        assertThat(reloadedEmail.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(eventResultRepository.findByEventId(emailEvent.getId())).isPresent();
    }

    @Test
    @DisplayName("7. A single event never receives duplicate result records")
    void shouldEnforceSingleResultPerEventAndPreventDuplicates() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("orderId", "ORD-DUP-CHECK");

        Event event = persistenceHelper.createAndAudit(
                "key-dup-check",
                new EventSubmissionRequest("ORDER_PLACED", payload, EventPriority.NORMAL)
        );

        // First process: succeeds and creates result
        workerManager.processNextBatch("worker-1", 5);

        Event completed = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(completed.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(eventResultRepository.findByEventId(event.getId())).isPresent();

        // Attempting to complete the event again via executionService must fail
        ProcessingResult duplicateResult = new ProcessingResult(payload, "Duplicate attempt");
        assertThatThrownBy(() -> executionService.completeEvent(event.getId(), duplicateResult, "worker-2"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Integrity check failed");

        // Attempting to directly insert a second EventResult violates the unique constraint
        EventResult directDuplicate = new EventResult(
                UUID.randomUUID(),
                event.getId(),
                payload,
                "Direct collision",
                "worker-3"
        );
        directDuplicate.setProcessedAt(Instant.now());

        assertThatThrownBy(() -> eventResultRepository.saveAndFlush(directDuplicate))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @DisplayName("8. End-to-end asynchronous processing with live background worker loop")
    void shouldProcessAsynchronouslyWithActiveWorkerPool() throws Exception {
        // Temporarily enable worker loop and start manager
        workerProperties.setEnabled(true);
        workerProperties.setPollingIntervalMs(50);
        workerProperties.setConcurrency(2);
        workerProperties.setBatchSize(2);
        workerManager.start();

        try {
            ObjectNode payload = JsonNodeFactory.instance.objectNode();
            payload.put("orderId", "ORD-ASYNC-LIVE");

            Event submitted = persistenceHelper.createAndAudit(
                    "key-async-live",
                    new EventSubmissionRequest("ORDER_PLACED", payload, EventPriority.HIGH)
            );

            // Wait deterministically until event reaches COMPLETED
            long deadline = System.currentTimeMillis() + 8000;
            Event latest = submitted;
            while (System.currentTimeMillis() < deadline) {
                latest = eventRepository.findById(submitted.getId()).orElseThrow();
                if (latest.getStatus() == EventStatus.COMPLETED) {
                    break;
                }
                Thread.sleep(100);
            }

            assertThat(latest.getStatus()).isEqualTo(EventStatus.COMPLETED);
            assertThat(latest.getAttemptCount()).isEqualTo((short) 1);

            EventResult result = eventResultRepository.findByEventId(submitted.getId()).orElseThrow();
            assertThat(result.getProcessedBy()).startsWith("worker-");

            List<EventAuditLog> auditLogs = auditLogRepository.findByEventIdOrderByOccurredAtAsc(submitted.getId());
            assertThat(auditLogs).hasSize(3);
            assertThat(auditLogs.get(1).getStatus()).isEqualTo(EventStatus.PROCESSING);
            assertThat(auditLogs.get(2).getStatus()).isEqualTo(EventStatus.COMPLETED);
        } finally {
            workerManager.stop();
            workerProperties.setEnabled(false);
        }
    }
}
