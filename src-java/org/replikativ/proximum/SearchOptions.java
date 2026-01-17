package org.replikativ.proximum;

/**
 * Options for search queries.
 *
 * <p>Use the builder to configure search behavior:</p>
 * <pre>{@code
 * SearchOptions options = SearchOptions.builder()
 *     .ef(100)  // Higher ef = better recall, slower
 *     .build();
 *
 * List<SearchResult> results = index.search(query, 10, options);
 * }</pre>
 */
public final class SearchOptions {

    /** Default saturation threshold for patience-based early termination (95% from paper) */
    public static final double DEFAULT_PATIENCE_SATURATION = 0.95;

    private final int ef;
    private final double minSimilarity;
    private final long timeoutNanos;
    private final int maxDistanceComputations;
    private final double patienceSaturation;
    private final int patience;

    private SearchOptions(int ef, double minSimilarity, long timeoutNanos,
                          int maxDistanceComputations, double patienceSaturation, int patience) {
        this.ef = ef;
        this.minSimilarity = minSimilarity;
        this.timeoutNanos = timeoutNanos;
        this.maxDistanceComputations = maxDistanceComputations;
        this.patienceSaturation = patienceSaturation;
        this.patience = patience;
    }

    /**
     * Create a new builder.
     *
     * @return builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Get the ef (beam width) parameter.
     *
     * @return ef value, or 0 if not set
     */
    public int getEf() {
        return ef;
    }

    /**
     * Get the minimum similarity threshold.
     *
     * @return minimum similarity, or 0 if not set
     */
    public double getMinSimilarity() {
        return minSimilarity;
    }

    /**
     * Get the timeout in nanoseconds.
     *
     * @return timeout in nanoseconds, or 0 if not set (no timeout)
     */
    public long getTimeoutNanos() {
        return timeoutNanos;
    }

    /**
     * Get the maximum number of distance computations allowed.
     *
     * @return max distance computations, or 0 if not set (no limit)
     */
    public int getMaxDistanceComputations() {
        return maxDistanceComputations;
    }

    /**
     * Get the patience saturation threshold.
     *
     * <p>Early termination triggers when result set stability exceeds this
     * threshold for {@link #getPatience()} consecutive iterations.</p>
     *
     * @return saturation threshold in [0, 1], or 0 if patience disabled
     */
    public double getPatienceSaturation() {
        return patienceSaturation;
    }

    /**
     * Get the patience parameter (consecutive saturated iterations before early termination).
     *
     * @return patience count, or 0 if patience disabled
     */
    public int getPatience() {
        return patience;
    }

    /**
     * Builder for SearchOptions.
     */
    public static class Builder {
        private int ef = 0;
        private double minSimilarity = 0;
        private long timeoutNanos = 0;
        private int maxDistanceComputations = 0;
        private double patienceSaturation = 0;
        private int patience = 0;

        private Builder() {}

        /**
         * Set the ef (beam width) for this search.
         *
         * <p>Higher values improve recall at the cost of latency.
         * Must be at least k (number of results requested).
         * If not set, uses the index's default ef-search.</p>
         *
         * @param ef beam width
         * @return this builder
         */
        public Builder ef(int ef) {
            this.ef = ef;
            return this;
        }

        /**
         * Set the minimum similarity threshold.
         *
         * <p>Results below this threshold are filtered out.
         * Value is in [0, 1] where 1 is most similar.</p>
         *
         * @param minSimilarity threshold
         * @return this builder
         */
        public Builder minSimilarity(double minSimilarity) {
            this.minSimilarity = minSimilarity;
            return this;
        }

        /**
         * Set a hard timeout for the search.
         *
         * <p>Search will terminate early if this timeout is exceeded,
         * returning the best results found so far. Checked every iteration.</p>
         *
         * @param timeoutNanos timeout in nanoseconds
         * @return this builder
         */
        public Builder timeoutNanos(long timeoutNanos) {
            this.timeoutNanos = timeoutNanos;
            return this;
        }

        /**
         * Set a hard timeout for the search in milliseconds.
         *
         * <p>Convenience method that converts to nanoseconds internally.</p>
         *
         * @param timeoutMillis timeout in milliseconds
         * @return this builder
         */
        public Builder timeoutMillis(long timeoutMillis) {
            this.timeoutNanos = timeoutMillis * 1_000_000L;
            return this;
        }

        /**
         * Set maximum number of distance computations.
         *
         * <p>Search will terminate early if this budget is exceeded,
         * returning the best results found so far.</p>
         *
         * @param maxDistanceComputations maximum distance computations allowed
         * @return this builder
         */
        public Builder maxDistanceComputations(int maxDistanceComputations) {
            this.maxDistanceComputations = maxDistanceComputations;
            return this;
        }

        /**
         * Enable patience-based early termination.
         *
         * <p>Implements "Patience in Proximumimity" (Teofili & Lin, ECIR 2025).
         * Search terminates early when result set stability exceeds the
         * saturation threshold for the specified number of consecutive iterations.</p>
         *
         * <p>Only applies after the result queue has filled to k elements.</p>
         *
         * @param saturationThreshold stability threshold in [0, 1], typically 0.95
         * @param patience consecutive saturated iterations before terminating
         * @return this builder
         */
        public Builder patience(double saturationThreshold, int patience) {
            this.patienceSaturation = saturationThreshold;
            this.patience = patience;
            return this;
        }

        /**
         * Enable patience-based early termination with default parameters.
         *
         * <p>Uses saturation threshold of 0.95 (from paper) and patience
         * scaled to max(7, k * 0.3).</p>
         *
         * <p>Note: patience will be computed at search time based on k.</p>
         *
         * @return this builder
         */
        public Builder patience() {
            this.patienceSaturation = DEFAULT_PATIENCE_SATURATION;
            this.patience = -1;  // Signal to compute from k at search time
            return this;
        }

        /**
         * Build the search options.
         *
         * @return configured options
         */
        public SearchOptions build() {
            return new SearchOptions(ef, minSimilarity, timeoutNanos,
                    maxDistanceComputations, patienceSaturation, patience);
        }
    }

    /**
     * Compute effective patience value, scaling with k if set to auto (-1).
     *
     * @param k number of results requested
     * @return effective patience value
     */
    public int getEffectivePatience(int k) {
        if (patience < 0) {
            // Auto-scale: max(7, k * 0.3)
            return Math.max(7, (int)(k * 0.3));
        }
        return patience;
    }
}
