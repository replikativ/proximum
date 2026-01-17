package org.replikativ.proximum;

/**
 * Configuration for a PersistentVectorIndex.
 *
 * <p>This is an immutable record of the configuration used to create an index.
 * Use {@link PersistentVectorIndex#builder()} to construct indexes.</p>
 */
public final class IndexConfig {

    private final int dimensions;
    private final int m;
    private final int efConstruction;
    private final int efSearch;
    private final long capacity;
    private final String storagePath;
    private final DistanceMetric distance;
    private final String branch;

    /**
     * Create an IndexConfig.
     */
    IndexConfig(int dimensions, int m, int efConstruction, int efSearch,
                long capacity, String storagePath, DistanceMetric distance, String branch) {
        this.dimensions = dimensions;
        this.m = m;
        this.efConstruction = efConstruction;
        this.efSearch = efSearch;
        this.capacity = capacity;
        this.storagePath = storagePath;
        this.distance = distance;
        this.branch = branch;
    }

    /**
     * Create a builder for IndexConfig (primarily for connect() use).
     */
    static Builder builder() {
        return new Builder();
    }

    public int getDimensions() { return dimensions; }
    public int getM() { return m; }
    public int getEfConstruction() { return efConstruction; }
    public int getEfSearch() { return efSearch; }
    public long getCapacity() { return capacity; }
    public String getStoragePath() { return storagePath; }
    public DistanceMetric getDistance() { return distance; }
    public String getBranch() { return branch; }

    @Override
    public String toString() {
        return "IndexConfig{" +
               "dimensions=" + dimensions +
               ", m=" + m +
               ", efConstruction=" + efConstruction +
               ", efSearch=" + efSearch +
               ", capacity=" + capacity +
               ", storagePath='" + storagePath + '\'' +
               ", distance=" + distance +
               ", branch='" + branch + '\'' +
               '}';
    }

    static class Builder {
        private int dimensions;
        private int m = 16;
        private int efConstruction = 200;
        private int efSearch = 50;
        private long capacity = 10_000_000;
        private String storagePath;
        private DistanceMetric distance = DistanceMetric.EUCLIDEAN;
        private String branch = "main";

        Builder dimensions(int d) { this.dimensions = d; return this; }
        Builder m(int m) { this.m = m; return this; }
        Builder efConstruction(int ef) { this.efConstruction = ef; return this; }
        Builder efSearch(int ef) { this.efSearch = ef; return this; }
        Builder capacity(long c) { this.capacity = c; return this; }
        Builder storagePath(String p) { this.storagePath = p; return this; }
        Builder distance(DistanceMetric d) { this.distance = d; return this; }
        Builder branch(String b) { this.branch = b; return this; }

        IndexConfig build() {
            return new IndexConfig(dimensions, m, efConstruction, efSearch,
                                   capacity, storagePath, distance, branch);
        }
    }
}
