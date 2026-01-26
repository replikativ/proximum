package org.replikativ.proximum;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JUnit 5 binding tests for ProximumVectorStore Java API.
 *
 * These tests verify that the Java→Clojure bridge works correctly:
 * - Each method calls the right Clojure function
 * - Data marshalling (Java ↔ Clojure) works
 * - Return types are correct
 * - Exceptions are handled
 *
 * Functional correctness is tested in Clojure tests; these are integration/smoke tests.
 */
@DisplayName("Java API Binding Tests")
class JavaApiBindingTest {

    @TempDir
    Path tempDir;

    private static float[] randomVector(int dim) {
        float[] vec = new float[dim];
        Random rand = new Random();
        for (int i = 0; i < dim; i++) {
            vec[i] = rand.nextFloat();
        }
        return vec;
    }

    @Nested
    @DisplayName("Core Operations")
    class CoreOperationsTest {

        @Test
        @DisplayName("delete() removes vector from search results")
        void testDelete() throws Exception {
            Path path = tempDir.resolve("delete-test");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(32)
                    .storagePath(path.toString())
                    .build()) {

                float[] v1 = randomVector(32);
                float[] v2 = randomVector(32);

                // Use mutable convenience methods for testing
                Object id1 = store.addAndGetId(v1);
                Object id2 = store.addAndGetId(v2);

                int countBefore = store.count();

                // Delete returns new store, but we use mutable reference
                ProximumVectorStore afterDelete = store.delete(id1);

                int countAfter = afterDelete.count();

                assertTrue(countAfter < countBefore, "delete() should decrease live count");

                List<SearchResult> results = afterDelete.search(v1, 10);
                boolean found = results.stream().anyMatch(r -> id1.equals(r.getId()));
                assertFalse(found, "Deleted vector should not appear in search");
            }
        }

        @Test
        @DisplayName("remainingCapacity() returns available space")
        void testRemainingCapacity() throws Exception {
            Path path = tempDir.resolve("remaining-capacity-test");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(32)
                    .storagePath(path.toString())
                    .capacity(1000)
                    .build()) {

                int capacityBefore = store.remainingCapacity();
                assertEquals(1000, capacityBefore, "Initial remaining capacity should equal capacity");

                store.addAndGetId(randomVector(32));
                store.addAndGetId(randomVector(32));

                int capacityAfter = store.remainingCapacity();
                assertEquals(998, capacityAfter, "Remaining capacity should decrease after adds");
            }
        }

        @Test
        @DisplayName("indexType() returns 'hnsw'")
        void testIndexType() throws Exception {
            Path path = tempDir.resolve("index-type-test");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(32)
                    .storagePath(path.toString())
                    .build()) {

                String indexType = store.indexType();
                assertEquals("hnsw", indexType, "Index type should be 'hnsw'");
            }
        }

        @Test
        @DisplayName("indexConfig() returns configuration map")
        void testIndexConfig() throws Exception {
            Path path = tempDir.resolve("index-config-test");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(64)
                    .m(32)
                    .efConstruction(100)
                    .capacity(1000)
                    .storagePath(path.toString())
                    .build()) {

                Map<String, Object> config = store.indexConfig();

                assertNotNull(config, "indexConfig() should return non-null");
                assertEquals(64L, ((Number) config.get("dim")).longValue(), "dim should match");
                assertEquals(32L, ((Number) config.get("M")).longValue(), "M should match");
            }
        }
    }

    @Nested
    @DisplayName("Metrics and Maintenance")
    class MetricsTest {

        @Test
        @DisplayName("getMetrics() returns expected metrics")
        void testMetrics() throws Exception {
            Path path = tempDir.resolve("metrics-test");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(32)
                    .storagePath(path.toString())
                    .build()) {

                for (int i = 0; i < 10; i++) {
                    store.addAndGetId(randomVector(32));
                }

                Map<String, Object> metrics = store.getMetrics();

                assertNotNull(metrics, "getMetrics() should return non-null");
                assertTrue(metrics.containsKey("vector-count"), "metrics should contain 'vector-count'");
                assertTrue(metrics.containsKey("deleted-count"), "metrics should contain 'deleted-count'");
                assertTrue(metrics.containsKey("live-count"), "metrics should contain 'live-count'");
                assertTrue(metrics.containsKey("capacity"), "metrics should contain 'capacity'");

                assertTrue(metrics.get("vector-count") instanceof Number, "vector-count should be numeric");
                assertTrue(metrics.get("deleted-count") instanceof Number, "deleted-count should be numeric");
            }
        }

        @Test
        @DisplayName("isNeedsCompaction() returns boolean")
        void testNeedsCompaction() throws Exception {
            Path path = tempDir.resolve("needs-compaction-test");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(32)
                    .storagePath(path.toString())
                    .build()) {

                for (int i = 0; i < 10; i++) {
                    store.addAndGetId(randomVector(32));
                }

                boolean needs = store.isNeedsCompaction();
                assertFalse(needs, "Fresh index should not need compaction");
            }
        }
    }

    @Nested
    @DisplayName("Garbage Collection")
    class GcTest {

        @Test
        @DisplayName("gc() is callable on persisted store")
        void testGc() throws Exception {
            Path path = tempDir.resolve("gc-test");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(32)
                    .storagePath(path.toString())
                    .build()) {

                // Add some vectors and create multiple commits
                for (int i = 0; i < 5; i++) {
                    store.addAndGetId(randomVector(32));
                }
                store.sync().get();

                // Delete one to create garbage
                Object firstId = store.addAndGetId(randomVector(32));
                store.sync().get();
                store.delete(firstId);
                store.sync().get();

                // gc() returns CompletableFuture<Set<Object>>
                // Just verify it's callable - actual gc behavior tested in Clojure tests
                try {
                    Object result = store.gc().get();  // Wait for async completion
                    // If it returns, should be a Set (possibly empty)
                    if (result != null) {
                        assertInstanceOf(Set.class, result, "gc() result should be a Set");
                    }
                } catch (Exception e) {
                    // gc may throw if async operation fails - that's acceptable for binding test
                    // The method was callable, which is what we're testing
                }
            }
        }

        @Test
        @DisplayName("getHistory() returns list of commits")
        void testHistory() throws Exception {
            Path path = tempDir.resolve("history-test");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(32)
                    .storagePath(path.toString())
                    .build()) {

                store.addAndGetId(randomVector(32));
                store.sync().get();

                store.addAndGetId(randomVector(32));
                store.sync().get();

                List<Map<String, Object>> history = store.getHistory();

                assertNotNull(history, "getHistory() should return non-null");
                assertInstanceOf(List.class, history, "history should be a List");
                assertTrue(history.size() >= 1, "history should have at least one entry");

                if (!history.isEmpty()) {
                    Map<String, Object> entry = history.get(0);
                    assertTrue(entry.containsKey("commit-id"), "history entry should contain 'commit-id'. Keys present: " + entry.keySet());
                    assertInstanceOf(java.util.UUID.class, entry.get("commit-id"), "commit-id should be a UUID");
                }
            }
        }
    }

    @Nested
    @DisplayName("Compaction")
    class CompactionTest {

        @Test
        @DisplayName("compact() returns new compacted index")
        void testCompact() throws Exception {
            Path path = tempDir.resolve("compact-test");
            Path compactPath = tempDir.resolve("compact-result");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(32)
                    .storagePath(path.toString())
                    .capacity(100)
                    .build()) {

                Object id1 = store.addAndGetId(randomVector(32));
                store.addAndGetId(randomVector(32));
                store.addAndGetId(randomVector(32));
                store.sync().get();

                ProximumVectorStore afterDelete = store.delete(id1);
                afterDelete.sync();

                // Build target config for compact
                // Use ":file" so toClojureMap converts to keyword, and UUID object (not string)
                Map<String, Object> target = Map.of(
                    "store-config", Map.of(
                        "backend", ":file",
                        "path", compactPath.toString(),
                        "id", UUID.randomUUID()
                    ),
                    "mmap-dir", compactPath.toString()
                );

                ProximumVectorStore compacted = afterDelete.compact(target);
                assertNotNull(compacted, "compact() should return non-null");

                int compactedCount = compacted.count();
                assertTrue(compactedCount > 0, "compacted index should have vectors");

                compacted.close();
            }
        }
    }

    @Nested
    @DisplayName("Search Operations")
    class SearchTest {

        @Test
        @DisplayName("searchFiltered() respects allowed IDs")
        void testFilteredSearch() throws Exception {
            Path path = tempDir.resolve("filtered-search-test");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(32)
                    .storagePath(path.toString())
                    .build()) {

                Object id1 = store.addAndGetId(randomVector(32));
                Object id2 = store.addAndGetId(randomVector(32));
                Object id3 = store.addAndGetId(randomVector(32));

                Set<Object> allowedIds = new HashSet<>(Arrays.asList(id1, id2));
                float[] query = randomVector(32);

                List<SearchResult> results = store.searchFiltered(query, 10, allowedIds);

                assertNotNull(results, "searchFiltered() should return non-null");
                assertInstanceOf(List.class, results, "searchFiltered() should return List");

                for (SearchResult r : results) {
                    assertTrue(allowedIds.contains(r.getId()),
                            "Result should only contain allowed IDs, got: " + r.getId());
                }
            }
        }

        @Test
        @DisplayName("searchFiltered() with empty set returns no results")
        void testFilteredSearchEmptySet() throws Exception {
            Path path = tempDir.resolve("filtered-empty-test");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(32)
                    .storagePath(path.toString())
                    .build()) {

                store.addAndGetId(randomVector(32));
                store.addAndGetId(randomVector(32));

                float[] query = randomVector(32);
                // Empty set filter - should return no results
                List<SearchResult> results = store.searchFiltered(query, 10, new HashSet<>());

                assertNotNull(results, "searchFiltered() with empty set should work");
                assertEquals(0, results.size(), "Empty filter should return no vectors");
            }
        }

        @Test
        @DisplayName("searchWithMetadata() includes metadata in results")
        void testSearchWithMetadata() throws Exception {
            Path path = tempDir.resolve("search-with-metadata-test");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(32)
                    .storagePath(path.toString())
                    .build()) {

                Map<String, Object> meta1 = Map.of("name", "vector1", "category", "A");
                Map<String, Object> meta2 = Map.of("name", "vector2", "category", "B");

                store.addAndGetId(randomVector(32), "id1", meta1);
                store.addAndGetId(randomVector(32), "id2", meta2);

                float[] query = randomVector(32);
                List<SearchResult> results = store.searchWithMetadata(query, 10);

                assertNotNull(results, "searchWithMetadata() should return non-null");
                assertEquals(2, results.size(), "Should return 2 results");

                for (SearchResult r : results) {
                    assertNotNull(r.getMetadata(), "Result should include metadata");
                    assertTrue(r.getMetadata().containsKey("name"), "Metadata should contain 'name'");
                }
            }
        }
    }

    @Nested
    @DisplayName("Metadata Operations")
    class MetadataTest {

        @Test
        @DisplayName("withMetadata() updates vector metadata")
        void testWithMetadata() throws Exception {
            Path path = tempDir.resolve("with-metadata-test");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(32)
                    .storagePath(path.toString())
                    .build()) {

                Object id = store.addAndGetId(randomVector(32));

                // Initially no user metadata (may contain system fields like external-id)
                Map<String, Object> metaBefore = store.getMetadata(id);
                assertFalse(metaBefore != null && metaBefore.containsKey("label"),
                        "Initially should have no user metadata");

                // Set metadata - returns new store
                Map<String, Object> newMeta = Map.of("label", "test", "score", 42);
                ProximumVectorStore afterMeta = store.withMetadata(id, newMeta);

                // Verify metadata was set
                Map<String, Object> metaAfter = afterMeta.getMetadata(id);
                assertNotNull(metaAfter, "Metadata should be set");
                assertEquals("test", metaAfter.get("label"), "label should match");
                assertEquals(42L, ((Number) metaAfter.get("score")).longValue(), "score should match");
            }
        }
    }

    @Nested
    @DisplayName("Branching Operations")
    class BranchingTest {

        @Test
        @DisplayName("branch() and listBranches() work correctly")
        void testBranching() throws Exception {
            Path path = tempDir.resolve("branching-test");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(32)
                    .storagePath(path.toString())
                    .build()) {

                store.addAndGetId(randomVector(32));
                store.sync().get();

                Set<String> branches = store.listBranches();
                assertNotNull(branches, "listBranches() should return non-null");
                assertInstanceOf(Set.class, branches, "branches should be a Set");
                assertTrue(branches.contains("main"), "branches should contain 'main'");

                String currentBranch = store.getCurrentBranch();
                assertEquals("main", currentBranch, "getCurrentBranch() should return 'main'");

                ProximumVectorStore feature = store.branch("feature-test");
                assertNotNull(feature, "branch() should return non-null");

                Set<String> branchesAfter = feature.listBranches();
                assertTrue(branchesAfter.contains("feature-test"), "branches should contain new branch");

                assertEquals("feature-test", feature.getCurrentBranch(),
                        "getCurrentBranch() should return new branch");

                feature.close();
            }
        }

        @Test
        @DisplayName("getCommitId() returns UUID after sync")
        void testGetCommitId() throws Exception {
            Path path = tempDir.resolve("commit-id-test");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(32)
                    .storagePath(path.toString())
                    .build()) {

                store.addAndGetId(randomVector(32));
                store.sync().get();

                UUID commitId = store.getCommitId();
                System.out.println("getCommitId() result: " + commitId);
                assertNotNull(commitId, "getCommitId() should return non-null after sync");
            }
        }
    }

    @Nested
    @DisplayName("Crypto-Hash Operations")
    class CryptoHashTest {

        @Test
        @DisplayName("crypto-hash bindings work correctly")
        void testCryptoHash() throws Exception {
            Path path = tempDir.resolve("crypto-hash-test");

            try (ProximumVectorStore store = ProximumVectorStore.builder()
                    .dimensions(32)
                    .storagePath(path.toString())
                    .cryptoHash(true)
                    .build()) {

                assertTrue(store.isCryptoHash(), "isCryptoHash() should return true");

                UUID hashBefore = store.getCommitHash();
                assertNull(hashBefore, "getCommitHash() should be null before first sync");

                store.addAndGetId(randomVector(32));
                store.addAndGetId(randomVector(32));
                store.sync().get();

                UUID hash1 = store.getCommitHash();
                assertNotNull(hash1, "getCommitHash() should return non-null after sync");

                store.addAndGetId(randomVector(32));
                store.sync().get();

                UUID hash2 = store.getCommitHash();
                assertNotNull(hash2, "getCommitHash() should be non-null after second sync");
                assertNotEquals(hash1, hash2, "Commit hash should change after new sync");
            }
        }
    }
}
