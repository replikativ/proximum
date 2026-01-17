package org.replikativ.proximum;

/**
 * Thrown when a vector's dimensions don't match the index configuration.
 */
public class DimensionMismatchException extends ProximumException {

    private final int expected;
    private final int actual;

    /**
     * Create a new dimension mismatch exception.
     *
     * @param expected expected dimensions
     * @param actual actual dimensions provided
     */
    public DimensionMismatchException(int expected, int actual) {
        super("Dimension mismatch: expected " + expected + ", got " + actual);
        this.expected = expected;
        this.actual = actual;
    }

    /**
     * Get the expected dimension count.
     *
     * @return expected dimensions
     */
    public int getExpected() {
        return expected;
    }

    /**
     * Get the actual dimension count that was provided.
     *
     * @return actual dimensions
     */
    public int getActual() {
        return actual;
    }
}
