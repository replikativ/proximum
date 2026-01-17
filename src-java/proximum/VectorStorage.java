package proximum;

import java.lang.foreign.MemorySegment;

/**
 * Interface for vector storage.
 *
 * Implementations must provide:
 * - append: add a new vector, return its ID
 * - get: retrieve a vector by ID
 * - getSegment: return MemorySegment for SIMD operations
 *
 * The storage layout must be compatible with SIMD distance computation:
 * - Vectors stored as contiguous little-endian floats
 * - Offset for vector i = HEADER_SIZE + (i * dim * 4)
 * - HEADER_SIZE = 64 bytes
 */
public interface VectorStorage {

    /** Header size in bytes (reserved for metadata) */
    int HEADER_SIZE = 64;

    /**
     * Append a vector to storage.
     *
     * @param vector float array of length dim
     * @return node ID for the appended vector
     */
    int append(float[] vector);

    /**
     * Get a vector by ID.
     *
     * @param nodeId the node ID
     * @return float array copy of the vector
     */
    float[] get(int nodeId);

    /**
     * Get the MemorySegment for SIMD operations.
     * The segment must be mapped for the lifetime of the index.
     *
     * @return MemorySegment containing all vectors
     */
    MemorySegment getSegment();

    /**
     * Get vector dimensionality.
     */
    int getDim();

    /**
     * Get current number of vectors.
     */
    int getCount();

    /**
     * Get maximum capacity.
     */
    int getCapacity();

    /**
     * Close the storage and release resources.
     */
    void close();
}
