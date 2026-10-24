package com.keerat.event_driven.retry;

import com.keerat.event_driven.config.RetryProperties;
import com.keerat.event_driven.domain.Event;
import com.keerat.event_driven.domain.EventAuditLog;
import com.keerat.event_driven.domain.EventPriority;
import com.keerat.event_driven.domain.EventResult;
import com.keerat.event_driven.domain.EventStatus;
import com.keerat.event_driven.dto.EventSubmissionRequest;
import com.keerat.event_driven.exception.InvalidEventStateException;
import com.keerat.event_driven.exception.StaleClaimException;
import com.keerat.event_driven.processor.ProcessingResult;
import com.keerat.event_driven.repository.EventAuditLogRepository;
import com.keerat.event_driven.repository.EventRepository;
import com.keerat.event_driven.repository.EventResultRepository;
import com.keerat.event_driven.service.DlqService;
import com.keerat.event_driven.service.EventPersistenceHelper;
import com.keerat.event_driven.worker.EventWorkerManager;
import com.keerat.event_driven.worker.service.EventClaimService;
import com.keerat.event_driven.worker.service.EventExecutionService;
import com.keerat.event_driven.worker.service.EventRecoveryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class EventRetryAndRecoveryIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

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
    private EventRecoveryService recoveryService;

    @Autowired
    private DlqService dlqService;

    @Autowired
    private RetryProperties retryProperties;

    @Autowired
    private RetryPolicyService retryPolicyService;

    @BeforeEach
    void setUp() {
        eventResultRepository.deleteAll();
        auditLogRepository.deleteAll();
        eventRepository.deleteAll();
        retryProperties.setEnabled(true);
        retryProperties.setDefaultMaxAttempts(5);
        retryProperties.setBaseDelay(Duration.ofSeconds(1));
        retryProperties.setMaxDelay(Duration.ofSeconds(60));
        retryProperties.setLeaseDuration(Duration.ofSeconds(30));
    }

    @Test
    @DisplayName("1. A retryable failure schedules a retry and increments attempt count")
    void shouldScheduleRetryOnRetryableFailure() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("orderId", "ORD-RETRY-1");
        payload.put("simulateFailure", "TRANSIENT");

        Event event = persistenceHelper.createAndAudit(
                "key-retry-1",
                new EventSubmissionRequest("ORDER_PLACED", payload, EventPriority.HIGH)
        );

        Instant before = Instant.now();
        int processed = workerManager.processNextBatch("worker-1", 5);
        assertThat(processed).isEqualTo(1);

        Event reloaded = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(reloaded.getAttemptCount()).isEqualTo((short) 1);
        assertThat(reloaded.getNextAttemptAt()).isAfter(before);
        assertThat(reloaded.getLastFailureReason()).contains("Simulated transient fulfillment queue timeout");
        assertThat(reloaded.getClaimToken()).isNull();
        assertThat(reloaded.getProcessingLeaseUntil()).isNull();

        // Audit log history: PENDING (submission) -> PROCESSING (attempt 1) -> PENDING (retry scheduled)
        List<EventAuditLog> logs = auditLogRepository.findByEventIdOrderByOccurredAtAsc(event.getId());
        assertThat(logs).hasSize(3);
        assertThat(logs.get(0).getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(logs.get(1).getStatus()).isEqualTo(EventStatus.PROCESSING);
        assertThat(logs.get(2).getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(logs.get(2).getMessage()).contains("Retry scheduled for attempt 2");
    }

    @Test
    @DisplayName("2. Exponential backoff increases exponentially and respects maximum delay")
    void shouldCalculateExponentialBackoffCorrectly() {
        retryProperties.setBaseDelay(Duration.ofSeconds(1));
        retryProperties.setMaxDelay(Duration.ofSeconds(60));

        // delay = min(baseDelay * 2^(attempt - 1), maxDelay)
        assertThat(retryPolicyService.calculateDelay(1)).isEqualTo(Duration.ofSeconds(1));   // 1 * 2^0 = 1s
        assertThat(retryPolicyService.calculateDelay(2)).isEqualTo(Duration.ofSeconds(2));   // 1 * 2^1 = 2s
        assertThat(retryPolicyService.calculateDelay(3)).isEqualTo(Duration.ofSeconds(4));   // 1 * 2^2 = 4s
        assertThat(retryPolicyService.calculateDelay(4)).isEqualTo(Duration.ofSeconds(8));   // 1 * 2^3 = 8s
        assertThat(retryPolicyService.calculateDelay(5)).isEqualTo(Duration.ofSeconds(16));  // 1 * 2^4 = 16s
        assertThat(retryPolicyService.calculateDelay(6)).isEqualTo(Duration.ofSeconds(32));  // 1 * 2^5 = 32s
        assertThat(retryPolicyService.calculateDelay(7)).isEqualTo(Duration.ofSeconds(60));  // 1 * 2^6 = 64s > 60s -> capped at 60s
        assertThat(retryPolicyService.calculateDelay(50)).isEqualTo(Duration.ofSeconds(60)); // overflow protection
    }

    @Test
    @DisplayName("3. An event is not claimed before next_attempt_at")
    void shouldNotClaimEventBeforeNextAttemptAt() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("orderId", "ORD-TIME-1");

        Event event = persistenceHelper.createAndAudit(
                "key-time-1",
                new EventSubmissionRequest("ORDER_PLACED", payload, EventPriority.HIGH)
        );

        // Set next_attempt_at in the future
        event.setNextAttemptAt(Instant.now().plus(Duration.ofHours(1)));
        eventRepository.save(event);

        // Attempt claim: should return 0 events
        List<Event> claimed = claimService.claimEvents("worker-1", 5);
        assertThat(claimed).isEmpty();

        // Update next_attempt_at to the past
        event.setNextAttemptAt(Instant.now().minus(Duration.ofSeconds(5)));
        eventRepository.save(event);

        // Now claim should succeed
        List<Event> claimedNow = claimService.claimEvents("worker-1", 5);
        assertThat(claimedNow).hasSize(1);
        assertThat(claimedNow.get(0).getId()).isEqualTo(event.getId());
    }

    @Test
    @DisplayName("4. A retryable event eventually reaches DLQ after exhausting maximum attempts")
    void shouldMoveToDlqAfterExhaustingMaxAttempts() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("orderId", "ORD-EXHAUST-1");
        payload.put("simulateFailure", "TRANSIENT");

        Event event = persistenceHelper.createAndAudit(
                "key-exhaust-1",
                new EventSubmissionRequest("ORDER_PLACED", payload, EventPriority.HIGH)
        );
        event.setMaxAttempts((short) 3);
        eventRepository.save(event);

        // Attempt 1: fails and retries
        workerManager.processNextBatch("worker-1", 1);
        Event after1 = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(after1.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(after1.getAttemptCount()).isEqualTo((short) 1);

        // Make immediately eligible for Attempt 2
        after1.setNextAttemptAt(Instant.now().minusSeconds(1));
        eventRepository.save(after1);

        // Attempt 2: fails and retries
        workerManager.processNextBatch("worker-1", 1);
        Event after2 = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(after2.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(after2.getAttemptCount()).isEqualTo((short) 2);

        // Make immediately eligible for Attempt 3
        after2.setNextAttemptAt(Instant.now().minusSeconds(1));
        eventRepository.save(after2);

        // Attempt 3: exhausted maxAttempts (3) -> transitions to DLQ!
        workerManager.processNextBatch("worker-1", 1);
        Event after3 = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(after3.getStatus()).isEqualTo(EventStatus.DLQ);
        assertThat(after3.getAttemptCount()).isEqualTo((short) 3);
        assertThat(after3.getLastFailureReason()).contains("Max attempts (3) exhausted");

        // Verify DLQ audit record exists
        List<EventAuditLog> logs = auditLogRepository.findByEventIdOrderByOccurredAtAsc(event.getId());
        EventAuditLog lastLog = logs.get(logs.size() - 1);
        assertThat(lastLog.getStatus()).isEqualTo(EventStatus.DLQ);
        assertThat(lastLog.getAttemptNumber()).isEqualTo((short) 3);
        assertThat(lastLog.getMessage()).contains("Max attempts (3) exhausted");
    }

    @Test
    @DisplayName("5. Permanent validation failures are not retried and transition directly to FAILED")
    void shouldNotRetryPermanentValidationFailures() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("orderId", "ORD-PERM-1");
        payload.put("simulateFailure", "PERMANENT");

        Event event = persistenceHelper.createAndAudit(
                "key-perm-1",
                new EventSubmissionRequest("ORDER_PLACED", payload, EventPriority.HIGH)
        );

        workerManager.processNextBatch("worker-1", 5);

        Event reloaded = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(reloaded.getNextAttemptAt()).isNull();
        assertThat(reloaded.getLastFailureReason()).contains("Simulated permanent business rule violation");

        // Audit history: PENDING -> PROCESSING -> FAILED
        List<EventAuditLog> logs = auditLogRepository.findByEventIdOrderByOccurredAtAsc(event.getId());
        assertThat(logs).hasSize(3);
        assertThat(logs.get(2).getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(logs.get(2).getMessage()).contains("Simulated permanent business rule violation");
    }

    @Test
    @DisplayName("6. A successful retry creates one result and transitions to COMPLETED")
    void shouldCreateOneResultAndTransitionToCompletedOnSuccessfulRetry() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("orderId", "ORD-RETRY-SUCCESS");
        payload.put("simulateFailure", "TRANSIENT");

        Event event = persistenceHelper.createAndAudit(
                "key-retry-success",
                new EventSubmissionRequest("ORDER_PLACED", payload, EventPriority.NORMAL)
        );

        // Attempt 1 fails transiently
        workerManager.processNextBatch("worker-1", 1);
        Event failedFirst = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(failedFirst.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(failedFirst.getAttemptCount()).isEqualTo((short) 1);

        // Fix transient condition: remove simulateFailure, reset next_attempt_at to past
        payload.remove("simulateFailure");
        failedFirst.setPayload(payload);
        failedFirst.setNextAttemptAt(Instant.now().minusSeconds(1));
        eventRepository.save(failedFirst);

        // Attempt 2 succeeds
        workerManager.processNextBatch("worker-1", 1);

        Event completed = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(completed.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(completed.getAttemptCount()).isEqualTo((short) 2);

        // Verify exactly one EventResult exists
        EventResult result = eventResultRepository.findByEventId(event.getId()).orElseThrow();
        assertThat(result.getResultMessage()).contains("ORD-RETRY-SUCCESS");

        // Audit logs: PENDING -> PROCESSING (attempt 1) -> PENDING (retry scheduled) -> PROCESSING (attempt 2) -> COMPLETED
        List<EventAuditLog> logs = auditLogRepository.findByEventIdOrderByOccurredAtAsc(event.getId());
        assertThat(logs).hasSize(5);
        assertThat(logs.get(4).getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(logs.get(4).getAttemptNumber()).isEqualTo((short) 2);
    }

    @Test
    @DisplayName("7. When automatic retries are disabled, retryable failures transition directly to FAILED")
    void shouldFailDirectlyWhenAutomaticRetriesDisabled() {
        retryProperties.setEnabled(false);

        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("orderId", "ORD-DISABLED-RETRY");
        payload.put("simulateFailure", "TRANSIENT");

        Event event = persistenceHelper.createAndAudit(
                "key-no-retry",
                new EventSubmissionRequest("ORDER_PLACED", payload, EventPriority.NORMAL)
        );

        workerManager.processNextBatch("worker-1", 1);

        Event failed = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(failed.getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(failed.getLastFailureReason()).contains("Automatic retries disabled");

        List<EventAuditLog> logs = auditLogRepository.findByEventIdOrderByOccurredAtAsc(event.getId());
        assertThat(logs.get(logs.size() - 1).getStatus()).isEqualTo(EventStatus.FAILED);
    }

    @Test
    @DisplayName("8 & 9. DLQ REST APIs: list, retrieve, and manual retry of eligible events")
    void shouldSupportDlqApisAndManualRetry() throws Exception {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("orderId", "ORD-DLQ-API");

        Event event = persistenceHelper.createAndAudit(
                "key-dlq-api",
                new EventSubmissionRequest("ORDER_PLACED", payload, EventPriority.HIGH)
        );
        event.setStatus(EventStatus.DLQ);
        event.setAttemptCount((short) 5);
        event.setLastFailureReason("Max attempts (5) exhausted");
        eventRepository.save(event);

        // 1. GET /api/v1/dlq (list paginated)
        mockMvc.perform(get("/api/v1/dlq"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content", hasSize(1)))
                .andExpect(jsonPath("$.content[0].id", is(event.getId().toString())))
                .andExpect(jsonPath("$.content[0].status", is("DLQ")));

        // 2. GET /api/v1/dlq/{eventId}
        mockMvc.perform(get("/api/v1/dlq/" + event.getId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is(event.getId().toString())))
                .andExpect(jsonPath("$.lastFailureReason", containsString("Max attempts (5) exhausted")));

        // 3. POST /api/v1/dlq/{eventId}/retry (successful manual retry)
        mockMvc.perform(post("/api/v1/dlq/" + event.getId() + "/retry"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("PENDING")))
                .andExpect(jsonPath("$.attemptCount", is(0)));

        // Verify event in DB is now PENDING with attemptCount 0
        Event requeued = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(requeued.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(requeued.getAttemptCount()).isEqualTo((short) 0);

        // Verify audit trail recorded dlq-operator action
        List<EventAuditLog> logs = auditLogRepository.findByEventIdOrderByOccurredAtAsc(event.getId());
        EventAuditLog retryAudit = logs.get(logs.size() - 1);
        assertThat(retryAudit.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(retryAudit.getWorkerId()).isEqualTo("dlq-operator");
        assertThat(retryAudit.getMessage()).contains("Manual retry requested from DLQ by operator");

        // 4. Invalid manual retries:
        // A) Retrying now that it is PENDING should return 409 Conflict
        mockMvc.perform(post("/api/v1/dlq/" + event.getId() + "/retry"))
                .andExpect(status().isConflict());

        // B) Non-existent ID returns 404
        mockMvc.perform(post("/api/v1/dlq/" + UUID.randomUUID() + "/retry"))
                .andExpect(status().isNotFound());

        // C) Retrying a FAILED (permanent) event returns 409 Conflict
        Event failedEvent = persistenceHelper.createAndAudit(
                "key-failed-retry",
                new EventSubmissionRequest("ORDER_PLACED", payload, EventPriority.LOW)
        );
        failedEvent.setStatus(EventStatus.FAILED);
        eventRepository.save(failedEvent);

        mockMvc.perform(post("/api/v1/dlq/" + failedEvent.getId() + "/retry"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message", containsString("permanent FAILED state")));
    }

    @Test
    @DisplayName("10. Concurrent manual retry requests cannot schedule duplicate claims")
    void shouldPreventDuplicateClaimsUnderConcurrentManualRetries() throws Exception {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("orderId", "ORD-CONC-DLQ");

        Event event = persistenceHelper.createAndAudit(
                "key-conc-dlq",
                new EventSubmissionRequest("ORDER_PLACED", payload, EventPriority.NORMAL)
        );
        event.setStatus(EventStatus.DLQ);
        event.setAttemptCount((short) 5);
        eventRepository.save(event);

        int threadCount = 3;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger conflictCount = new AtomicInteger(0);
        List<Throwable> unexpectedErrors = Collections.synchronizedList(new ArrayList<>());

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    dlqService.retryDlqEvent(event.getId());
                    successCount.incrementAndGet();
                } catch (InvalidEventStateException e) {
                    conflictCount.incrementAndGet();
                } catch (Throwable e) {
                    unexpectedErrors.add(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean finished = doneLatch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(finished).isTrue();
        assertThat(unexpectedErrors).isEmpty();
        // Exactly one thread succeeded, all other threads got rejected
        assertThat(successCount.get()).isEqualTo(1);
        assertThat(conflictCount.get()).isEqualTo(threadCount - 1);

        Event updated = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(updated.getStatus()).isEqualTo(EventStatus.PENDING);
    }

    @Test
    @DisplayName("11. Expired PROCESSING claims can be recovered safely")
    void shouldRecoverExpiredProcessingClaims() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("orderId", "ORD-RECOVERY-1");

        Event event = persistenceHelper.createAndAudit(
                "key-recovery-1",
                new EventSubmissionRequest("ORDER_PLACED", payload, EventPriority.HIGH)
        );
        event.setStatus(EventStatus.PROCESSING);
        event.setAttemptCount((short) 1);
        event.setClaimToken(UUID.randomUUID());
        // Lease expired 10 seconds ago
        event.setProcessingLeaseUntil(Instant.now().minusSeconds(10));
        eventRepository.save(event);

        int recovered = recoveryService.recoverExpiredProcessingEvents(10);
        assertThat(recovered).isEqualTo(1);

        Event afterRecovery = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(afterRecovery.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(afterRecovery.getClaimToken()).isNull();
        assertThat(afterRecovery.getProcessingLeaseUntil()).isNull();
        assertThat(afterRecovery.getNextAttemptAt()).isNotNull();

        // Audit log reflects recovery
        List<EventAuditLog> logs = auditLogRepository.findByEventIdOrderByOccurredAtAsc(event.getId());
        EventAuditLog recoveryLog = logs.get(logs.size() - 1);
        assertThat(recoveryLog.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(recoveryLog.getWorkerId()).isEqualTo("recovery-coordinator");
        assertThat(recoveryLog.getMessage()).contains("Processing lease expired; recovered");
    }

    @Test
    @DisplayName("12. Stale workers cannot overwrite results or status of newer claims")
    void shouldPreventStaleWorkerFromOverwritingClaim() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("orderId", "ORD-STALE-CHECK");

        Event event = persistenceHelper.createAndAudit(
                "key-stale-check",
                new EventSubmissionRequest("ORDER_PLACED", payload, EventPriority.HIGH)
        );

        // Worker 1 claims event
        UUID staleClaimToken = UUID.randomUUID();
        event.setStatus(EventStatus.PROCESSING);
        event.setAttemptCount((short) 1);
        event.setClaimToken(staleClaimToken);
        event.setProcessingLeaseUntil(Instant.now().minusSeconds(10)); // expired
        eventRepository.save(event);

        // Crash recovery runs and revokes Worker 1's claim
        recoveryService.recoverExpiredProcessingEvents(5);

        // Worker 2 claims event with fresh claim token
        UUID newClaimToken = UUID.randomUUID();
        Event reloaded = eventRepository.findById(event.getId()).orElseThrow();
        reloaded.setStatus(EventStatus.PROCESSING);
        reloaded.setAttemptCount((short) 2);
        reloaded.setClaimToken(newClaimToken);
        reloaded.setProcessingLeaseUntil(Instant.now().plusSeconds(30));
        eventRepository.save(reloaded);

        // Now stale Worker 1 attempts to complete event with staleClaimToken
        ProcessingResult staleResult = new ProcessingResult(payload, "Stale worker completion");
        assertThatThrownBy(() -> executionService.completeEvent(event.getId(), staleClaimToken, staleResult, "worker-1"))
                .isInstanceOf(StaleClaimException.class)
                .hasMessageContaining("claim token mismatch");

        // Verify EventResult was NOT written
        assertThat(eventResultRepository.findByEventId(event.getId())).isEmpty();

        // Stale Worker 1 attempts to record failure with staleClaimToken
        assertThatThrownBy(() -> executionService.recordPermanentFailure(event.getId(), staleClaimToken, "Stale failure", "worker-1"))
                .isInstanceOf(StaleClaimException.class);

        // Active Worker 2 completes event with valid newClaimToken -> succeeds!
        ProcessingResult validResult = new ProcessingResult(payload, "Valid worker-2 completion");
        executionService.completeEvent(event.getId(), newClaimToken, validResult, "worker-2");

        Event finalEvent = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(finalEvent.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(eventResultRepository.findByEventId(event.getId())).isPresent();
    }
}
