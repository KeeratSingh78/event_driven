package com.keerat.event_driven.worker;

import com.keerat.event_driven.config.RetryProperties;
import com.keerat.event_driven.config.WorkerProperties;
import com.keerat.event_driven.domain.Event;
import com.keerat.event_driven.exception.StaleClaimException;
import com.keerat.event_driven.processor.BusinessProcessingService;
import com.keerat.event_driven.processor.ProcessingResult;
import com.keerat.event_driven.retry.FailureClassifier;
import com.keerat.event_driven.retry.RetryPolicyService;
import com.keerat.event_driven.worker.service.EventClaimService;
import com.keerat.event_driven.worker.service.EventExecutionService;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages the background worker pool lifecycle, asynchronous polling loops,
 * and dispatch to business processors with retry/DLQ classification.
 *
 * <p>Implements {@link SmartLifecycle} with {@code getPhase() = Integer.MAX_VALUE}
 * ensuring that workers only start after all databases and Flyway migrations are completely ready.
 */
@Component
public class EventWorkerManager implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(EventWorkerManager.class);

    private final WorkerProperties properties;
    private final RetryProperties retryProperties;
    private final RetryPolicyService retryPolicyService;
    private final EventClaimService claimService;
    private final BusinessProcessingService businessProcessingService;
    private final EventExecutionService executionService;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executorService;

    public EventWorkerManager(WorkerProperties properties,
                              RetryProperties retryProperties,
                              RetryPolicyService retryPolicyService,
                              EventClaimService claimService,
                              BusinessProcessingService businessProcessingService,
                              EventExecutionService executionService) {
        this.properties = properties;
        this.retryProperties = retryProperties;
        this.retryPolicyService = retryPolicyService;
        this.claimService = claimService;
        this.businessProcessingService = businessProcessingService;
        this.executionService = executionService;
    }

    @Override
    public synchronized void start() {
        if (!properties.isEnabled()) {
            log.info("Background event workers are disabled by configuration (app.worker.enabled=false)");
            return;
        }

        if (running.compareAndSet(false, true)) {
            int concurrency = Math.max(1, properties.getConcurrency());
            log.info("Starting {} background event worker(s) (pollingIntervalMs={}, batchSize={})",
                    concurrency, properties.getPollingIntervalMs(), properties.getBatchSize());

            AtomicInteger threadNumber = new AtomicInteger(1);
            ThreadFactory threadFactory = r -> {
                Thread t = new Thread(r, "event-worker-" + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            };

            executorService = Executors.newFixedThreadPool(concurrency, threadFactory);

            for (int i = 1; i <= concurrency; i++) {
                String workerId = "worker-" + i;
                executorService.submit(() -> workerLoop(workerId));
            }
        }
    }

    @Override
    @PreDestroy
    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Shutting down background event worker pool...");
            if (executorService != null) {
                executorService.shutdown();
                try {
                    if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                        log.warn("Worker pool did not terminate within 5 seconds, forcing shutdown");
                        executorService.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    executorService.shutdownNow();
                }
            }
            log.info("Background event worker pool stopped");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return properties.isEnabled();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    /**
     * Continuous polling loop executed by each worker thread.
     */
    private void workerLoop(String workerId) {
        log.info("Worker thread {} started", workerId);

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                int processedCount = processNextBatch(workerId, properties.getBatchSize());
                if (processedCount == 0) {
                    Thread.sleep(properties.getPollingIntervalMs());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Worker thread {} interrupted, shutting down loop", workerId);
                break;
            } catch (Exception e) {
                log.error("Worker {} encountered an unexpected error during cycle: {}", workerId, e.getMessage(), e);
                try {
                    Thread.sleep(Math.min(properties.getPollingIntervalMs(), 1000));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.info("Worker thread {} terminated", workerId);
    }

    /**
     * Claims up to {@code batchSize} events and processes them synchronously.
     * Useful for deterministic testing and worker loop execution.
     *
     * @param workerId  the worker identifier
     * @param batchSize the maximum batch size to claim
     * @return the number of events processed
     */
    public int processNextBatch(String workerId, int batchSize) {
        List<Event> claimedEvents = claimService.claimEvents(workerId, batchSize);
        if (claimedEvents.isEmpty()) {
            return 0;
        }

        for (Event event : claimedEvents) {
            processEvent(event, workerId);
        }

        return claimedEvents.size();
    }

    /**
     * Executes business processing and state transition for a single claimed event.
     * Fences stale claims and routes outcomes to COMPLETED, PENDING (retry), DLQ, or FAILED.
     *
     * @param event    the claimed event
     * @param workerId the worker executing the work
     */
    public void processEvent(Event event, String workerId) {
        if (event == null) {
            return;
        }

        UUID claimToken = event.getClaimToken();

        try {
            // Business processing executes outside of database transactions
            ProcessingResult result = businessProcessingService.process(event);

            // Commit outcome and audit entry in short dedicated transaction
            executionService.completeEvent(event.getId(), claimToken, result, workerId);
        } catch (StaleClaimException sce) {
            log.warn("Worker {} claim on event {} was stale (expired or reassigned): {}",
                    workerId, event.getId(), sce.getMessage());
        } catch (Throwable t) {
            handleProcessingFailure(event, claimToken, workerId, t);
        }
    }

    private void handleProcessingFailure(Event event, UUID claimToken, String workerId, Throwable t) {
        String reason = t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
        boolean retryable = FailureClassifier.isRetryable(t);

        try {
            if (!retryable) {
                log.warn("Permanent failure processing event {} (worker={}): {}",
                        event.getId(), workerId, reason);
                executionService.recordPermanentFailure(event.getId(), claimToken, reason, workerId);
            } else if (!retryProperties.isEnabled()) {
                log.warn("Retryable failure for event {}, but automatic retries disabled (worker={}): {}",
                        event.getId(), workerId, reason);
                executionService.recordPermanentFailure(event.getId(), claimToken,
                        "Automatic retries disabled; " + reason, workerId);
            } else if (retryPolicyService.isExhausted(event.getAttemptCount(), event.getMaxAttempts())) {
                log.warn("Event {} exhausted max attempts ({}); moving to DLQ (worker={}): {}",
                        event.getId(), event.getMaxAttempts(), workerId, reason);
                executionService.moveToDlq(event.getId(), claimToken, reason, workerId);
            } else {
                Instant nextAttemptAt = retryPolicyService.calculateNextAttemptAt(event.getAttemptCount());
                log.info("Scheduling retry for event {} attempt {} at {} (worker={}): {}",
                        event.getId(), event.getAttemptCount() + 1, nextAttemptAt, workerId, reason);
                executionService.scheduleRetry(event.getId(), claimToken, reason, nextAttemptAt, workerId);
            }
        } catch (StaleClaimException sce) {
            log.warn("Worker {} could not record failure for event {}: claim was stale: {}",
                    workerId, event.getId(), sce.getMessage());
        } catch (Exception ex) {
            log.error("Failed to persist failure transition for event {}: {}",
                    event.getId(), ex.getMessage(), ex);
        }
    }
}
