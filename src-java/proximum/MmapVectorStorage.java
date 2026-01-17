package proximum;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;

/**
 * Memory-mapped vector storage for HNSW index.
 *
 * Provides:
 * - Fast append via mmap
 * - SIMD-compatible memory layout (little-endian floats)
 * - MemorySegment for direct SIMD access
 *
 * File format:
 * - Header (64 bytes): [dim (4), count (4), capacity (4), reserved...]
 * - Vectors: dim * 4 bytes each, contiguous
 */
public final class MmapVectorStorage implements VectorStorage {

    private static final ValueLayout.OfFloat FLOAT_LE =
        ValueLayout.JAVA_FLOAT.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfInt INT_LE =
        ValueLayout.JAVA_INT.withOrder(ByteOrder.LITTLE_ENDIAN);

    private final Path path;
    private final int dim;
    private final int capacity;
    private final AtomicInteger count;
    private final FileChannel channel;
    private final MemorySegment segment;
    private final Arena arena;
    private volatile boolean closed = false;

    /**
     * Create a new vector storage at the given path.
     *
     * @param path path to mmap file (will be created if not exists)
     * @param dim vector dimensionality
     * @param capacity maximum number of vectors
     */
    public MmapVectorStorage(Path path, int dim, int capacity) {
        this.path = path;
        this.dim = dim;
        this.capacity = capacity;
        this.count = new AtomicInteger(0);

        try {
            // Ensure parent directory exists
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }

            // Calculate file size
            long fileSize = HEADER_SIZE + (long) capacity * dim * 4L;

            // Open/create file
            this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

            // Resize if needed
            if (channel.size() < fileSize) {
                channel.position(fileSize - 1);
                channel.write(java.nio.ByteBuffer.allocate(1));
            }

            // Map to memory segment
            this.arena = Arena.ofShared();
            this.segment = channel.map(
                FileChannel.MapMode.READ_WRITE, 0, fileSize, arena);

            // Initialize header
            segment.set(INT_LE, 0, dim);
            segment.set(INT_LE, 4, 0);  // count
            segment.set(INT_LE, 8, capacity);

        } catch (Exception e) {
            throw new RuntimeException("Failed to create vector storage", e);
        }
    }

    /**
     * Open existing vector storage.
     *
     * @param path path to existing mmap file
     */
    public static MmapVectorStorage open(Path path) {
        try {
            FileChannel channel = FileChannel.open(path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

            Arena arena = Arena.ofShared();
            MemorySegment seg = channel.map(
                FileChannel.MapMode.READ_WRITE, 0, channel.size(), arena);

            int dim = seg.get(INT_LE, 0);
            int count = seg.get(INT_LE, 4);
            int capacity = seg.get(INT_LE, 8);

            MmapVectorStorage storage = new MmapVectorStorage(path, dim, capacity, count, channel, seg, arena);
            return storage;

        } catch (Exception e) {
            throw new RuntimeException("Failed to open vector storage", e);
        }
    }

    // Private constructor for opening existing storage
    private MmapVectorStorage(Path path, int dim, int capacity, int count,
                              FileChannel channel, MemorySegment segment, Arena arena) {
        this.path = path;
        this.dim = dim;
        this.capacity = capacity;
        this.count = new AtomicInteger(count);
        this.channel = channel;
        this.segment = segment;
        this.arena = arena;
    }

    @Override
    public int append(float[] vector) {
        if (closed) {
            throw new IllegalStateException("Storage is closed");
        }
        if (vector.length != dim) {
            throw new IllegalArgumentException(
                "Vector dimension mismatch: expected " + dim + ", got " + vector.length);
        }

        int nodeId = count.getAndIncrement();
        if (nodeId >= capacity) {
            count.decrementAndGet();
            throw new IllegalStateException("Storage capacity exceeded");
        }

        long base = HEADER_SIZE + (long) nodeId * dim * 4L;

        FloatBuffer fb = segment.asSlice(base, (long) dim * 4L)
                .asByteBuffer()
                .order(ByteOrder.LITTLE_ENDIAN)
                .asFloatBuffer();

        fb.put(vector, 0, dim);

        // Update count in header
        segment.set(INT_LE, 4, nodeId + 1);

        return nodeId;
    }

    @Override
    public float[] get(int nodeId) {
        if (closed) {
            throw new IllegalStateException("Storage is closed");
        }
        if (nodeId < 0 || nodeId >= count.get()) {
            throw new IndexOutOfBoundsException("Invalid node ID: " + nodeId);
        }

        long base = HEADER_SIZE + (long) nodeId * dim * 4L;
        MemorySegment slice = segment.asSlice(base, (long) dim * 4);

        ByteBuffer bb = slice.asByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
        float[] out = new float[dim];
        bb.asFloatBuffer().get(out);
        return out;
    }

    @Override
    public MemorySegment getSegment() {
        return segment;
    }

    @Override
    public int getDim() {
        return dim;
    }

    @Override
    public int getCount() {
        return count.get();
    }

    @Override
    public int getCapacity() {
        return capacity;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;

        try {
            // Sync final count to header
            segment.set(INT_LE, 4, count.get());

            // Force writes to disk
            segment.force();

            // Close arena (unmaps segment)
            arena.close();

            // Close channel
            channel.close();

        } catch (Exception e) {
            throw new RuntimeException("Failed to close storage", e);
        }
    }

    /**
     * Delete the storage file.
     * Must be called after close().
     */
    public void delete() {
        try {
            Files.deleteIfExists(path);
        } catch (Exception e) {
            // Ignore
        }
    }

    /**
     * Force sync to disk.
     */
    public void sync() {
        segment.set(INT_LE, 4, count.get());
        segment.force();
    }
}
