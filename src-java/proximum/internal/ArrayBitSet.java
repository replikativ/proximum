package proximum.internal;

/**
 * Word-aligned bitset for O(1) visited node tracking.
 *
 * Uses 32-bit words for efficient bit manipulation.
 *
 * <p><b>Internal API</b> - subject to change without notice.
 */
public final class ArrayBitSet {
    private final int[] buffer;

    public ArrayBitSet(int count) {
        // Allocate enough 32-bit words to hold 'count' bits
        // Over-allocate by 1 word to avoid bounds checks
        this.buffer = new int[(count >> 5) + 1];
    }

    /**
     * Check if bit at index is set.
     */
    public boolean contains(int bitIndex) {
        int wordIndex = bitIndex >> 5;  // Divide by 32
        if (wordIndex >= buffer.length) return false;
        int word = this.buffer[wordIndex];
        return ((1 << (bitIndex & 31)) & word) != 0;
    }

    /**
     * Set bit at index.
     */
    public void add(int id) {
        int wordIndex = id >> 5;
        if (wordIndex < buffer.length) {
            int mask = 1 << (id & 31);
            this.buffer[wordIndex] |= mask;
        }
    }

    /**
     * Clear all bits.
     */
    public void clear() {
        java.util.Arrays.fill(buffer, 0);
    }

    /**
     * Remove (clear) bit at index.
     */
    public void remove(int id) {
        int wordIndex = id >> 5;
        if (wordIndex < buffer.length) {
            int mask = ~(1 << (id & 31));
            this.buffer[wordIndex] &= mask;
        }
    }

    /**
     * Create a copy of this bitset.
     */
    public ArrayBitSet clone() {
        ArrayBitSet copy = new ArrayBitSet(buffer.length << 5);
        System.arraycopy(buffer, 0, copy.buffer, 0, buffer.length);
        return copy;
    }

    /**
     * Count the number of set bits.
     */
    public int cardinality() {
        int count = 0;
        for (int word : buffer) {
            count += Integer.bitCount(word);
        }
        return count;
    }
}
