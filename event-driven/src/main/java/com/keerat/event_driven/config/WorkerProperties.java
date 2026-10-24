package com.keerat.event_driven.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Externalized configuration for asynchronous event background workers.
 */
@Component
@ConfigurationProperties(prefix = "app.worker")
public class WorkerProperties {

    /**
     * Whether background event processing is enabled.
     */
    private boolean enabled = true;

    /**
     * Number of concurrent background worker threads.
     */
    private int concurrency = 3;

    /**
     * Polling interval in milliseconds when no eligible events are found.
     */
    private long pollingIntervalMs = 1000;

    /**
     * Maximum number of events to claim per batch.
     */
    private int batchSize = 5;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public long getPollingIntervalMs() {
        return pollingIntervalMs;
    }

    public void setPollingIntervalMs(long pollingIntervalMs) {
        this.pollingIntervalMs = pollingIntervalMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
