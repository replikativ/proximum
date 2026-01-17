package org.replikativ.proximum;

/**
 * Distance metrics for vector similarity comparison.
 *
 * <p>The choice of metric depends on your embedding model:</p>
 * <ul>
 *   <li><b>EUCLIDEAN:</b> L2 distance. Use for general-purpose embeddings.</li>
 *   <li><b>COSINE:</b> Angle-based distance. Use for normalized embeddings (OpenAI, most transformers).</li>
 *   <li><b>INNER_PRODUCT:</b> Dot product (negated). Use when magnitude matters.</li>
 * </ul>
 */
public enum DistanceMetric {

    /**
     * Euclidean (L2) distance.
     *
     * <p>Measures straight-line distance between vectors.
     * Lower values indicate higher similarity.</p>
     */
    EUCLIDEAN,

    /**
     * Cosine distance (1 - cosine similarity).
     *
     * <p>Measures angular difference between vectors, ignoring magnitude.
     * Ideal for normalized embeddings. Lower values indicate higher similarity.</p>
     */
    COSINE,

    /**
     * Inner product distance (negated dot product).
     *
     * <p>Measures alignment and magnitude. For normalized vectors,
     * equivalent to cosine similarity. Higher values (less negative)
     * indicate higher similarity.</p>
     */
    INNER_PRODUCT
}
