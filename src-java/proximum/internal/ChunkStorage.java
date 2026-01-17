package proximum.internal;

/**
 * Storage interface for lazy chunk loading in PersistentEdgeStore.
 *
 * Implementations can back chunks with any storage (konserve, mmap, etc).
 * When a chunk is accessed but not in memory, restore() is called to load it.
 *
 * <p><b>Internal API</b> - subject to change without notice.
 */
public interface ChunkStorage {

    /**
     * Restore a chunk from storage by its address.
     *
     * @param address The storage address (returned by prior store() call)
     * @return The chunk data, or null if not found
     */
    int[] restore(long address);

    /**
     * Signal that a chunk was accessed (for LRU tracking).
     * Called on cache hits to update access time.
     *
     * @param address The storage address
     */
    default void accessed(long address) {
        // Default: no-op. Implementations can track for LRU eviction.
    }

    /**
     * Store a chunk and return its address.
     * Used during sync to persist dirty chunks.
     *
     * @param encodedAddress The encoded address (layer << 32 | chunkIdx)
     * @param chunk The chunk data to store
     * @return The storage address (can be same as encodedAddress)
     */
    long store(long encodedAddress, int[] chunk);
}
