package com.keerat.event_driven.worker;

import com.keerat.event_driven.config.RetryProperties;
import com.keerat.event_driven.worker.service.EventRecoveryService;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Periodically triggers crash recovery sweeps for expired processing leases.
 */
@Component
public class EventRecoveryManager implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(EventRecoveryManager.class);

    private final RetryProperties retryProperties;
    private final EventRecoveryService recoveryService;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledExecutorService scheduledExecutor;

    public EventRecoveryManager(RetryProperties retryProperties, EventRecoveryService recoveryService) {
        this.retryProperties = retryProperties;
        this.recoveryService = recoveryService;
    }

    @Override
    public synchronized void start() {
        if (!retryProperties.isRecoveryEnabled()) {
            log.info("Background crash recovery scanner is disabled (app.retry.recovery-enabled=false)");
            return;
        }

        if (running.compareAndSet(false, true)) {
            long intervalMillis = retryProperties.getRecoveryInterval().toMillis();
            log.info("Starting background crash recovery scanner (interval={}ms)", intervalMillis);

            scheduledExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "event-crash-recovery");
                t.setDaemon(true);
                return t;
            });

            scheduledExecutor.scheduleWithFixedDelay(this::runRecoverySweep,
                    intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    @PreDestroy
    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping background crash recovery scanner...");
            if (scheduledExecutor != null) {
                scheduledExecutor.shutdown();
                try {
                    if (!scheduledExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                        scheduledExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    scheduledExecutor.shutdownNow();
                }
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return retryProperties.isRecoveryEnabled();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    /**
     * Executes one recovery sweep.
     */
    public int runRecoverySweep() {
        try {
            int recovered = recoveryService.recoverExpiredProcessingEvents(50);
            if (recovered > 0) {
                log.info("Crash recovery sweep recovered {} stranded event(s)", recovered);
            }
            return recovered;
        } catch (Exception e) {
            log.error("Error during crash recovery sweep: {}", e.getMessage(), e);
            return 0;
        }
    }
}
