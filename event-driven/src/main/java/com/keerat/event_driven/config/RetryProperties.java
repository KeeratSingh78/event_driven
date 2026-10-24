package com.keerat.event_driven.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Configuration properties for retry policies, exponential backoff,
 * processing leases, and worker crash recovery.
 */
@Component
@ConfigurationProperties(prefix = "app.retry")
public class RetryProperties {

    /**
     * Whether automatic retries are enabled across the platform.
     */
    private boolean enabled = true;

    /**
     * Default maximum processing attempts per event if not overridden on the event itself.
     */
    private int defaultMaxAttempts = 5;

    /**
     * Base delay for exponential backoff (e.g. 1 second).
     */
    private Duration baseDelay = Duration.ofSeconds(1);

    /**
     * Maximum capped delay for exponential backoff (e.g. 60 seconds).
     */
    private Duration maxDelay = Duration.ofSeconds(60);

    /**
     * Processing lease duration granted to a worker upon claiming an event.
     * If uncompleted past this duration, the claim is eligible for crash recovery.
     */
    private Duration leaseDuration = Duration.ofSeconds(30);

    /**
     * Whether background crash recovery scanner is enabled.
     */
    private boolean recoveryEnabled = true;

    /**
     * Interval between background recovery sweeps for expired processing leases.
     */
    private Duration recoveryInterval = Duration.ofSeconds(5);

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getDefaultMaxAttempts() {
        return defaultMaxAttempts;
    }

    public void setDefaultMaxAttempts(int defaultMaxAttempts) {
        this.defaultMaxAttempts = defaultMaxAttempts;
    }

    public Duration getBaseDelay() {
        return baseDelay;
    }

    public void setBaseDelay(Duration baseDelay) {
        this.baseDelay = baseDelay;
    }

    public Duration getMaxDelay() {
        return maxDelay;
    }

    public void setMaxDelay(Duration maxDelay) {
        this.maxDelay = maxDelay;
    }

    public Duration getLeaseDuration() {
        return leaseDuration;
    }

    public void setLeaseDuration(Duration leaseDuration) {
        this.leaseDuration = leaseDuration;
    }

    public boolean isRecoveryEnabled() {
        return recoveryEnabled;
    }

    public void setRecoveryEnabled(boolean recoveryEnabled) {
        this.recoveryEnabled = recoveryEnabled;
    }

    public Duration getRecoveryInterval() {
        return recoveryInterval;
    }

    public void setRecoveryInterval(Duration recoveryInterval) {
        this.recoveryInterval = recoveryInterval;
    }
}
