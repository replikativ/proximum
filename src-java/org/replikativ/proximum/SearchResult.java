package org.replikativ.proximum;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a search result from a vector similarity query.
 *
 * <p>Results are ordered by distance (closest first). Lower distance
 * values indicate higher similarity.</p>
 *
 * <p>The ID is the external/document ID provided when the vector was added.
 * It can be any type (Long for Datahike, String, UUID, etc.).</p>
 */
public final class SearchResult {

    private final Object id;
    private final double distance;
    private Map<String, Object> metadata;

    /**
     * Create a search result.
     *
     * @param id the external/document ID (can be Long, String, UUID, etc.)
     * @param distance the distance from the query vector
     */
    public SearchResult(Object id, double distance) {
        this.id = id;
        this.distance = distance;
    }

    /**
     * Create a search result with metadata.
     *
     * @param id the external/document ID (can be Long, String, UUID, etc.)
     * @param distance the distance from the query vector
     * @param metadata the vector's metadata
     */
    public SearchResult(Object id, double distance, Map<String, Object> metadata) {
        this.id = id;
        this.distance = distance;
        this.metadata = metadata;
    }

    /**
     * Get the external/document ID.
     *
     * <p>The ID type depends on what was provided when adding the vector.
     * For Datahike integration this is typically Long, but can also be
     * String, UUID, or any other serializable type.</p>
     *
     * @return the external ID
     */
    public Object getId() {
        return id;
    }

    /**
     * Get the distance from the query vector.
     *
     * <p>Interpretation depends on the distance metric:</p>
     * <ul>
     *   <li>EUCLIDEAN: Lower is more similar (0 = identical)</li>
     *   <li>COSINE: Lower is more similar (0 = identical direction)</li>
     *   <li>INNER_PRODUCT: Higher (less negative) is more similar</li>
     * </ul>
     *
     * @return the distance
     */
    public double getDistance() {
        return distance;
    }

    /**
     * Get the similarity score (inverse of distance).
     *
     * <p>Normalized to [0, 1] range where 1 is most similar.</p>
     *
     * @return similarity score
     */
    public double getSimilarity() {
        // For euclidean/cosine: 1 / (1 + distance)
        // This gives a nice [0, 1] range
        return 1.0 / (1.0 + distance);
    }

    /**
     * Get the metadata associated with this vector.
     *
     * @return metadata map, or null if not loaded
     */
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    /**
     * Set the metadata (used internally when loading).
     *
     * @param metadata the metadata map
     */
    void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchResult that = (SearchResult) o;
        return Objects.equals(id, that.id) && Double.compare(distance, that.distance) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, distance);
    }

    @Override
    public String toString() {
        return "SearchResult{id=" + id + ", distance=" + distance +
               (metadata != null ? ", metadata=" + metadata : "") + "}";
    }
}
