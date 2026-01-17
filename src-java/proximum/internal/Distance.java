package proximum.internal;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * SIMD-accelerated distance computations for HNSW.
 *
 * <p>All methods are static and final for maximum JIT inlining.
 * The JIT will inline these identically to hand-inlined code.
 *
 * <p><b>Internal API</b> - subject to change without notice.
 */
public final class Distance {

    private Distance() {} // Prevent instantiation

    // =========================================================================
    // Constants
    // =========================================================================

    /** Euclidean (L2 squared) distance. */
    public static final int EUCLIDEAN = 0;

    /** Cosine distance (1 - cosine similarity). Requires normalized vectors. */
    public static final int COSINE = 1;

    /** Inner product distance (1 - dot product). */
    public static final int INNER_PRODUCT = 2;

    // =========================================================================
    // SIMD Setup
    // =========================================================================

    static final VectorSpecies<Float> FLOAT_SPECIES = FloatVector.SPECIES_PREFERRED;
    static final int SPECIES_LENGTH = FLOAT_SPECIES.length();
    static final ValueLayout.OfFloat FLOAT_LE =
        ValueLayout.JAVA_FLOAT.withOrder(ByteOrder.LITTLE_ENDIAN);

    /** Header size in vector storage (must match VectorStore). */
    public static final int HEADER_SIZE = 64;

    // =========================================================================
    // Vector Normalization
    // =========================================================================

    /**
     * Normalize a vector in place (L2 normalization).
     * After normalization, ||vector|| = 1.
     *
     * @param vector The vector to normalize (modified in place)
     */
    public static void normalizeVector(float[] vector) {
        int dim = vector.length;
        int upperBound = dim - (dim % SPECIES_LENGTH);

        // SIMD computation of squared norm
        FloatVector normVec = FloatVector.zero(FLOAT_SPECIES);
        for (int i = 0; i < upperBound; i += SPECIES_LENGTH) {
            FloatVector v = FloatVector.fromArray(FLOAT_SPECIES, vector, i);
            normVec = normVec.add(v.mul(v));
        }
        float normSq = normVec.reduceLanes(VectorOperators.ADD);

        // Scalar tail
        for (int i = upperBound; i < dim; i++) {
            normSq += vector[i] * vector[i];
        }

        // Normalize if non-zero
        if (normSq > 1e-12f) {
            float invNorm = (float) (1.0 / Math.sqrt(normSq));

            // SIMD normalization
            FloatVector invNormVec = FloatVector.broadcast(FLOAT_SPECIES, invNorm);
            for (int i = 0; i < upperBound; i += SPECIES_LENGTH) {
                FloatVector v = FloatVector.fromArray(FLOAT_SPECIES, vector, i);
                v.mul(invNormVec).intoArray(vector, i);
            }

            // Scalar tail
            for (int i = upperBound; i < dim; i++) {
                vector[i] *= invNorm;
            }
        }
    }

    /**
     * Normalize multiple vectors in place (L2 normalization).
     *
     * @param vectors Array of vectors to normalize (each modified in place)
     */
    public static void normalizeVectors(float[][] vectors) {
        for (float[] vector : vectors) {
            normalizeVector(vector);
        }
    }

    // =========================================================================
    // Node-to-Query Distance (MemorySegment node vs float[] query)
    // =========================================================================

    /**
     * Squared Euclidean distance from stored node to query vector.
     * SIMD-accelerated, reads directly from memory-mapped segment.
     */
    public static double euclideanSquared(MemorySegment seg, int nodeId, int dim, float[] query) {
        long offset = HEADER_SIZE + (long) nodeId * dim * 4L;

        int upperBound = dim - (dim % SPECIES_LENGTH);
        FloatVector sumVec = FloatVector.zero(FLOAT_SPECIES);

        for (int i = 0; i < upperBound; i += SPECIES_LENGTH) {
            FloatVector va = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg,
                offset + i * 4L, ByteOrder.LITTLE_ENDIAN);
            FloatVector vb = FloatVector.fromArray(FLOAT_SPECIES, query, i);
            FloatVector diff = va.sub(vb);
            sumVec = sumVec.add(diff.mul(diff));
        }

        double sum = sumVec.reduceLanes(VectorOperators.ADD);

        for (int j = upperBound; j < dim; j++) {
            float a = seg.get(FLOAT_LE, offset + j * 4L);
            float d = a - query[j];
            sum += d * d;
        }

        return sum;
    }

    /**
     * Inner product distance from stored node to query vector.
     * Returns 1 - dot(a, b). For normalized vectors, equals cosine distance.
     */
    public static double innerProduct(MemorySegment seg, int nodeId, int dim, float[] query) {
        long offset = HEADER_SIZE + (long) nodeId * dim * 4L;

        int upperBound = dim - (dim % SPECIES_LENGTH);
        FloatVector dotVec = FloatVector.zero(FLOAT_SPECIES);

        for (int i = 0; i < upperBound; i += SPECIES_LENGTH) {
            FloatVector va = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg,
                offset + i * 4L, ByteOrder.LITTLE_ENDIAN);
            FloatVector vb = FloatVector.fromArray(FLOAT_SPECIES, query, i);
            dotVec = dotVec.add(va.mul(vb));
        }

        double dot = dotVec.reduceLanes(VectorOperators.ADD);

        for (int j = upperBound; j < dim; j++) {
            float a = seg.get(FLOAT_LE, offset + j * 4L);
            dot += a * query[j];
        }

        return 1.0 - dot;
    }

    /**
     * Compute distance from stored node to query using specified metric.
     *
     * @param distanceType EUCLIDEAN, COSINE, or INNER_PRODUCT
     */
    public static double compute(MemorySegment seg, int nodeId, int dim, float[] query, int distanceType) {
        return switch (distanceType) {
            case COSINE, INNER_PRODUCT -> innerProduct(seg, nodeId, dim, query);
            default -> euclideanSquared(seg, nodeId, dim, query);
        };
    }

    // =========================================================================
    // Node-to-Node Distance (both in MemorySegment)
    // =========================================================================

    /**
     * Squared Euclidean distance between two stored nodes.
     */
    public static double euclideanSquaredNodes(MemorySegment seg, int dim, int nodeA, int nodeB) {
        long offsetA = HEADER_SIZE + (long) nodeA * dim * 4L;
        long offsetB = HEADER_SIZE + (long) nodeB * dim * 4L;

        int upperBound = dim - (dim % SPECIES_LENGTH);
        FloatVector sumVec = FloatVector.zero(FLOAT_SPECIES);

        for (int i = 0; i < upperBound; i += SPECIES_LENGTH) {
            FloatVector va = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg,
                offsetA + i * 4L, ByteOrder.LITTLE_ENDIAN);
            FloatVector vb = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg,
                offsetB + i * 4L, ByteOrder.LITTLE_ENDIAN);
            FloatVector diff = va.sub(vb);
            sumVec = sumVec.add(diff.mul(diff));
        }

        double sum = sumVec.reduceLanes(VectorOperators.ADD);

        for (int j = upperBound; j < dim; j++) {
            float a = seg.get(FLOAT_LE, offsetA + j * 4L);
            float b = seg.get(FLOAT_LE, offsetB + j * 4L);
            float d = a - b;
            sum += d * d;
        }

        return sum;
    }

    /**
     * Inner product distance between two stored nodes.
     */
    public static double innerProductNodes(MemorySegment seg, int dim, int nodeA, int nodeB) {
        long offsetA = HEADER_SIZE + (long) nodeA * dim * 4L;
        long offsetB = HEADER_SIZE + (long) nodeB * dim * 4L;

        int upperBound = dim - (dim % SPECIES_LENGTH);
        FloatVector dotVec = FloatVector.zero(FLOAT_SPECIES);

        for (int i = 0; i < upperBound; i += SPECIES_LENGTH) {
            FloatVector va = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg,
                offsetA + i * 4L, ByteOrder.LITTLE_ENDIAN);
            FloatVector vb = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg,
                offsetB + i * 4L, ByteOrder.LITTLE_ENDIAN);
            dotVec = dotVec.add(va.mul(vb));
        }

        double dot = dotVec.reduceLanes(VectorOperators.ADD);

        for (int j = upperBound; j < dim; j++) {
            float a = seg.get(FLOAT_LE, offsetA + j * 4L);
            float b = seg.get(FLOAT_LE, offsetB + j * 4L);
            dot += a * b;
        }

        return 1.0 - dot;
    }

    /**
     * Compute distance between two stored nodes using specified metric.
     */
    public static double computeNodes(MemorySegment seg, int dim, int nodeA, int nodeB, int distanceType) {
        return switch (distanceType) {
            case COSINE, INNER_PRODUCT -> innerProductNodes(seg, dim, nodeA, nodeB);
            default -> euclideanSquaredNodes(seg, dim, nodeA, nodeB);
        };
    }

    // =========================================================================
    // Array-to-Array Distance (both float[])
    // =========================================================================

    /**
     * Squared Euclidean distance between two float arrays.
     */
    public static double euclideanSquaredVectors(float[] a, float[] b) {
        int dim = a.length;
        int upperBound = dim - (dim % SPECIES_LENGTH);

        FloatVector sumVec = FloatVector.zero(FLOAT_SPECIES);
        for (int i = 0; i < upperBound; i += SPECIES_LENGTH) {
            FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, i);
            FloatVector vb = FloatVector.fromArray(FLOAT_SPECIES, b, i);
            FloatVector diff = va.sub(vb);
            sumVec = sumVec.add(diff.mul(diff));
        }
        double sum = sumVec.reduceLanes(VectorOperators.ADD);

        for (int i = upperBound; i < dim; i++) {
            float d = a[i] - b[i];
            sum += d * d;
        }

        return sum;
    }

    /**
     * Inner product distance between two float arrays.
     */
    public static double innerProductVectors(float[] a, float[] b) {
        int dim = a.length;
        int upperBound = dim - (dim % SPECIES_LENGTH);

        FloatVector dotVec = FloatVector.zero(FLOAT_SPECIES);
        for (int i = 0; i < upperBound; i += SPECIES_LENGTH) {
            FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, i);
            FloatVector vb = FloatVector.fromArray(FLOAT_SPECIES, b, i);
            dotVec = dotVec.add(va.mul(vb));
        }
        double dot = dotVec.reduceLanes(VectorOperators.ADD);

        for (int i = upperBound; i < dim; i++) {
            dot += a[i] * b[i];
        }

        return 1.0 - dot;
    }

    /**
     * Compute distance between two float arrays using specified metric.
     */
    public static double computeVectors(float[] a, float[] b, int distanceType) {
        return switch (distanceType) {
            case COSINE, INNER_PRODUCT -> innerProductVectors(a, b);
            default -> euclideanSquaredVectors(a, b);
        };
    }
}
