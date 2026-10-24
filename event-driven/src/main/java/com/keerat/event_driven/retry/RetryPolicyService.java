package com.keerat.event_driven.retry;

import com.keerat.event_driven.config.RetryProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Service calculating exponential backoff retry schedules with overflow protection
 * and injectable time support.
 */
@Service
public class RetryPolicyService {

    private final RetryProperties retryProperties;
    private final Clock clock;

    @Autowired
    public RetryPolicyService(RetryProperties retryProperties) {
        this(retryProperties, Clock.systemUTC());
    }

    public RetryPolicyService(RetryProperties retryProperties, Clock clock) {
        this.retryProperties = retryProperties;
        this.clock = clock != null ? clock : Clock.systemUTC();
    }

    /**
     * Calculates the exponential backoff delay based on the attempt count just completed:
     * {@code delay = min(baseDelay * 2^(attemptCount - 1), maxDelay)}.
     *
     * <p>Uses safe math to prevent integer or long overflow.
     *
     * @param attemptCount the completed attempt number (>= 1)
     * @return the calculated delay Duration
     */
    public Duration calculateDelay(int attemptCount) {
        Duration baseDelay = retryProperties.getBaseDelay();
        Duration maxDelay = retryProperties.getMaxDelay();

        if (attemptCount <= 1) {
            return baseDelay;
        }

        // Cap shift at 30 to avoid overflow when shifting 1L
        int shift = Math.min(attemptCount - 1, 30);
        long multiplier = 1L << shift;

        long baseMillis = baseDelay.toMillis();
        long maxMillis = maxDelay.toMillis();

        if (baseMillis > 0 && multiplier > maxMillis / baseMillis) {
            return maxDelay;
        }

        long calculatedMillis = baseMillis * multiplier;
        return Duration.ofMillis(Math.min(calculatedMillis, maxMillis));
    }

    /**
     * Calculates the next attempt timestamp based on the current clock.
     *
     * @param attemptCount the completed attempt count
     * @return the next attempt Instant
     */
    public Instant calculateNextAttemptAt(int attemptCount) {
        return calculateNextAttemptAt(attemptCount, clock.instant());
    }

    /**
     * Calculates the next attempt timestamp relative to a given reference time.
     *
     * @param attemptCount  the completed attempt count
     * @param referenceTime reference timestamp (typically failure time)
     * @return the next attempt Instant
     */
    public Instant calculateNextAttemptAt(int attemptCount, Instant referenceTime) {
        Duration delay = calculateDelay(attemptCount);
        return referenceTime.plus(delay);
    }

    /**
     * Checks if an event with the given attempt count has exhausted its allowed attempts.
     *
     * @param currentAttempts the current attempt count
     * @param maxAttempts     the event's maximum attempts
     * @return true if currentAttempts >= maxAttempts
     */
    public boolean isExhausted(int currentAttempts, int maxAttempts) {
        int allowed = maxAttempts > 0 ? maxAttempts : retryProperties.getDefaultMaxAttempts();
        return currentAttempts >= allowed;
    }
}
