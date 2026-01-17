package org.replikativ.proximum;

/**
 * Base exception for vector database operations.
 *
 * <p>All proximum exceptions extend this class,
 * making it easy to catch all database-related errors.</p>
 */
public class ProximumException extends RuntimeException {

    /**
     * Create a new exception with a message.
     *
     * @param message error message
     */
    public ProximumException(String message) {
        super(message);
    }

    /**
     * Create a new exception with a message and cause.
     *
     * @param message error message
     * @param cause underlying cause
     */
    public ProximumException(String message, Throwable cause) {
        super(message, cause);
    }
}
