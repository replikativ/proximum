package org.replikativ.proximum;

/**
 * Thrown when the index capacity has been reached.
 *
 * <p>To fix, create a new index with a larger capacity setting.</p>
 */
public class CapacityExceededException extends ProximumException {

    /**
     * Create a new capacity exceeded exception.
     *
     * @param message error message
     */
    public CapacityExceededException(String message) {
        super(message);
    }

    /**
     * Create a new capacity exceeded exception with cause.
     *
     * @param message error message
     * @param cause underlying cause
     */
    public CapacityExceededException(String message, Throwable cause) {
        super(message, cause);
    }
}
