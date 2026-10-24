package com.keerat.event_driven.retry;

import com.keerat.event_driven.processor.BusinessProcessingException;
import com.keerat.event_driven.processor.PermanentProcessingException;
import com.keerat.event_driven.processor.RetryableProcessingException;

/**
 * Utility classifying exceptions into permanent (non-retryable) or transient (retryable) categories.
 */
public final class FailureClassifier {

    private FailureClassifier() {
    }

    /**
     * Classifies a failure as retryable or permanent:
     * <ul>
     *   <li>{@link PermanentProcessingException} -> non-retryable</li>
     *   <li>{@link RetryableProcessingException} -> retryable</li>
     *   <li>{@link BusinessProcessingException} -> checks {@code isRetryable()}</li>
     *   <li>General unhandled exceptions (e.g. transient DB/network errors) -> retryable</li>
     * </ul>
     *
     * @param throwable the exception to classify
     * @return true if retryable, false if permanent
     */
    public static boolean isRetryable(Throwable throwable) {
        if (throwable == null) {
            return false;
        }
        if (throwable instanceof PermanentProcessingException) {
            return false;
        }
        if (throwable instanceof RetryableProcessingException) {
            return true;
        }
        if (throwable instanceof BusinessProcessingException bpe) {
            return bpe.isRetryable();
        }
        return true;
    }
}
