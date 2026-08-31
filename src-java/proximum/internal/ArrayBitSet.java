package proximum.internal;

import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * Word-aligned bitset for O(1) visited node tracking.
 *
 * Uses 32-bit words for efficient bit manipulation.
 *
 * <p><b>Internal API</b> - subject to change without notice.
 */
public final class ArrayBitSet {
    private final int[] buffer;
    private final NavigableSet<Integer> nonZeroWords = new TreeSet<>();
    private int cardinality;

    public ArrayBitSet(int count) {
        // Allocate enough 32-bit words to hold 'count' bits
        // Over-allocate by 1 word to avoid bounds checks
        this.buffer = new int[(count >> 5) + 1];
    }

    /**
     * Check if bit at index is set.
     */
    public boolean contains(int bitIndex) {
        if (bitIndex < 0) return false;
        int wordIndex = bitIndex >> 5;  // Divide by 32
        if (wordIndex >= buffer.length) return false;
        int word = this.buffer[wordIndex];
        return ((1 << (bitIndex & 31)) & word) != 0;
    }

    /**
     * Set bit at index.
     */
    public void add(int id) {
        if (id < 0) return;
        int wordIndex = id >> 5;
        if (wordIndex < buffer.length) {
            int mask = 1 << (id & 31);
            int oldWord = this.buffer[wordIndex];
            if ((oldWord & mask) == 0) {
                this.buffer[wordIndex] = oldWord | mask;
                cardinality++;
                if (oldWord == 0) {
                    nonZeroWords.add(wordIndex);
                }
            }
        }
    }

    /**
     * Clear all bits.
     */
    public void clear() {
        java.util.Arrays.fill(buffer, 0);
        nonZeroWords.clear();
        cardinality = 0;
    }

    /**
     * Remove (clear) bit at index.
     */
    public void remove(int id) {
        if (id < 0) return;
        int wordIndex = id >> 5;
        if (wordIndex < buffer.length) {
            int bit = 1 << (id & 31);
            int oldWord = this.buffer[wordIndex];
            if ((oldWord & bit) != 0) {
                int newWord = oldWord & ~bit;
                this.buffer[wordIndex] = newWord;
                cardinality--;
                if (newWord == 0) {
                    nonZeroWords.remove(wordIndex);
                }
            }
        }
    }

    /**
     * Create a copy of this bitset.
     */
    public ArrayBitSet clone() {
        ArrayBitSet copy = new ArrayBitSet(buffer.length << 5);
        System.arraycopy(buffer, 0, copy.buffer, 0, buffer.length);
        copy.nonZeroWords.addAll(nonZeroWords);
        copy.cardinality = cardinality;
        return copy;
    }

    /**
     * Count the number of set bits.
     */
    public int cardinality() {
        return cardinality;
    }

    /**
     * Return the first set bit at or after {@code fromIndex}, or -1.
     *
     * This keeps sparse exact-filter scans proportional to the allowed set
     * instead of probing every possible node ID.
     */
    public int nextSetBit(int fromIndex) {
        if (fromIndex < 0) {
            fromIndex = 0;
        }
        int wordIndex = fromIndex >> 5;
        if (wordIndex >= buffer.length) {
            return -1;
        }

        int word = buffer[wordIndex] & (-1 << (fromIndex & 31));
        if (word != 0) {
            return (wordIndex << 5) + Integer.numberOfTrailingZeros(word);
        }
        Integer nextWordIndex = nonZeroWords.ceiling(wordIndex + 1);
        return nextWordIndex == null
                ? -1
                : (nextWordIndex << 5)
                    + Integer.numberOfTrailingZeros(buffer[nextWordIndex]);
    }
}
