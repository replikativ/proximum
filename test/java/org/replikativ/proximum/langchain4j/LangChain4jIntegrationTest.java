package org.replikativ.proximum.langchain4j;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.filter.Filter;
import dev.langchain4j.store.embedding.filter.MetadataFilterBuilder;
import org.replikativ.proximum.DistanceMetric;
import org.replikativ.proximum.langchain4j.ProximumEmbeddingStore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/**
 * Integration test for LangChain4j EmbeddingStore adapter.
 *
 * Run with: clj -T:build run-adapter-compliance-tests
 */
public class LangChain4jIntegrationTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== LangChain4j EmbeddingStore Integration Test ===\n");

        // With strict Konserve semantics, create-store must fail if the target directory exists.
        // So we create a temp parent directory and choose a child path that does not yet exist.
        Path tempParent = Files.createTempDirectory("langchain4j-test");
        Path tempDir = tempParent.resolve("store");

        try {
            // Build the EmbeddingStore
            ProximumEmbeddingStore store = ProximumEmbeddingStore.builder()
                .storagePath(tempDir.toString())
                .dimensions(128)
                .distance(DistanceMetric.COSINE)
                .capacity(1000)
                .build();

            System.out.println("1. Created ProximumEmbeddingStore");

            // Test add with auto-generated ID
            Embedding emb1 = Embedding.from(generateEmbedding("The quick brown fox"));
            TextSegment seg1 = TextSegment.from("The quick brown fox",
                dev.langchain4j.data.document.Metadata.from("category", "animals"));
            String id1 = store.add(emb1, seg1);
            System.out.println("2. Added embedding with auto-ID: " + id1);

            // Test add with specified ID
            Embedding emb2 = Embedding.from(generateEmbedding("Machine learning"));
            TextSegment seg2 = TextSegment.from("Machine learning",
                dev.langchain4j.data.document.Metadata.from("category", "tech"));
            store.add("custom-id-2", emb2);
            System.out.println("3. Added embedding with custom ID: custom-id-2");

            // Test addAll
            List<Embedding> embeddings = List.of(
                Embedding.from(generateEmbedding("Vector databases")),
                Embedding.from(generateEmbedding("Semantic search"))
            );
            List<TextSegment> segments = List.of(
                TextSegment.from("Vector databases"),
                TextSegment.from("Semantic search")
            );
            List<String> batchIds = store.addAll(embeddings, segments);
            System.out.println("4. Added batch, IDs: " + batchIds);
            System.out.println("   Total count: " + store.count());

            // Test search
            Embedding queryEmb = Embedding.from(generateEmbedding("AI technology"));
            EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
                .queryEmbedding(queryEmb)
                .maxResults(3)
                .build();

            EmbeddingSearchResult<TextSegment> results = store.search(request);
            System.out.println("5. Search results for 'AI technology':");
            for (EmbeddingMatch<TextSegment> match : results.matches()) {
                System.out.printf("   - %s: %.3f score, text='%s'%n",
                    match.embeddingId(),
                    match.score(),
                    match.embedded() != null ? match.embedded().text() : "N/A");
            }

            // Test filtering semantics using official LangChain4j filter builder
            // (this validates our adapter uses Filter.test(Metadata) semantics)
            Filter techOnly = MetadataFilterBuilder.metadataKey("category").isEqualTo("tech");
            EmbeddingSearchRequest filteredRequest = EmbeddingSearchRequest.builder()
                .queryEmbedding(queryEmb)
                .filter(techOnly)
                .maxResults(10)
                .build();
            EmbeddingSearchResult<TextSegment> filteredResults = store.search(filteredRequest);
            System.out.println("5b. Filtered results (category == tech):");
            for (EmbeddingMatch<TextSegment> match : filteredResults.matches()) {
                System.out.printf("   - %s: %.3f score, text='%s'%n",
                    match.embeddingId(),
                    match.score(),
                    match.embedded() != null ? match.embedded().text() : "N/A");

                // We inserted metadata only for seg1 (animals) and custom-id-2 (tech).
                // Batch segments had no metadata.
                if (!"custom-id-2".equals(match.embeddingId())) {
                    throw new AssertionError("Filtered results must only contain tech items; got: " + match.embeddingId());
                }
            }

            // Complex filter coverage tests
            runComplexFilterCoverageTests(tempDir);

            // Test remove
            store.remove(id1);
            System.out.println("6. Removed " + id1 + ", count: " + store.count());

            // Test removeAll
            store.removeAll(batchIds);
            System.out.println("7. Removed batch, count: " + store.count());

            // Verify interface compliance
            assert store instanceof EmbeddingStore : "Must implement EmbeddingStore";
            System.out.println("8. Implements EmbeddingStore<TextSegment>: ✓");

            // Test sync and close
            store.sync();
            store.close();
            System.out.println("9. Synced and closed");

            System.out.println("\n=== LangChain4j Integration Test PASSED ===");

        } finally {
            deleteRecursively(tempParent);
        }
    }

    private static float[] generateEmbedding(String text) {
        float[] embedding = new float[128];
        int hash = text.hashCode();
        for (int i = 0; i < 128; i++) {
            embedding[i] = (float) Math.sin(hash + i * 0.1);
        }
        // Normalize
        float norm = 0;
        for (float v : embedding) norm += v * v;
        norm = (float) Math.sqrt(norm);
        for (int i = 0; i < 128; i++) embedding[i] /= norm;
        return embedding;
    }

    private static void runComplexFilterCoverageTests(Path parentTempDir) throws Exception {
        System.out.println("5c. Complex filter coverage (AND/OR/NOT, IN/NOT IN, numeric comparisons)");

        Path filterDir = parentTempDir.resolve("langchain4j-filters-" + System.currentTimeMillis());
        ProximumEmbeddingStore store = ProximumEmbeddingStore.builder()
            .storagePath(filterDir.toString())
            .dimensions(8)
            .distance(DistanceMetric.COSINE)
            .capacity(100)
            .build();

        try {
            // Use identical embeddings so similarity ranking doesn't hide items.
            Embedding constant = Embedding.from(constantEmbedding(8));
            Embedding query = Embedding.from(constantEmbedding(8));

            dev.langchain4j.data.document.Metadata mA = new dev.langchain4j.data.document.Metadata()
                .put("category", "tech")
                .put("year", 2020)
                .put("rating", 4.8d);
            dev.langchain4j.data.document.Metadata mB = new dev.langchain4j.data.document.Metadata()
                .put("category", "tech")
                .put("year", 2021)
                .put("rating", 4.2d);
            dev.langchain4j.data.document.Metadata mC = new dev.langchain4j.data.document.Metadata()
                .put("category", "animals")
                .put("year", 2019)
                .put("rating", 4.9d);
            dev.langchain4j.data.document.Metadata mD = new dev.langchain4j.data.document.Metadata()
                .put("category", "science")
                .put("year", 2023)
                .put("rating", 4.7d);
            dev.langchain4j.data.document.Metadata mE = new dev.langchain4j.data.document.Metadata()
                .put("category", "tech")
                .put("year", 2018)
                .put("rating", 3.0d);
            dev.langchain4j.data.document.Metadata mF = new dev.langchain4j.data.document.Metadata()
                .put("category", "misc")
                .put("year", 2022)
                .put("rating", 4.8d);
            dev.langchain4j.data.document.Metadata mG = new dev.langchain4j.data.document.Metadata()
                .put("category", "tech")
                .put("year", 2024)
                .put("rating", 4.9d);
            dev.langchain4j.data.document.Metadata mH = new dev.langchain4j.data.document.Metadata()
                .put("category", "other")
                .put("year", 2020)
                .put("rating", 2.0d);

            List<String> ids = List.of("a", "b", "c", "d", "e", "f", "g", "h");
            List<Embedding> embeddings = List.of(constant, constant, constant, constant, constant, constant, constant, constant);
            List<TextSegment> segments = List.of(
                TextSegment.from("a", mA),
                TextSegment.from("b", mB),
                TextSegment.from("c", mC),
                TextSegment.from("d", mD),
                TextSegment.from("e", mE),
                TextSegment.from("f", mF),
                TextSegment.from("g", mG),
                TextSegment.from("h", mH)
            );
            store.addAll(ids, embeddings, segments);

            // 1) category == tech
            Filter techOnly = MetadataFilterBuilder.metadataKey("category").isEqualTo("tech");
            assertFilterMatches(store, query, techOnly, Set.of("a", "b", "e", "g"));

            // 2) category == tech AND year >= 2020
            Filter techRecent = Filter.and(
                MetadataFilterBuilder.metadataKey("category").isEqualTo("tech"),
                MetadataFilterBuilder.metadataKey("year").isGreaterThanOrEqualTo(2020)
            );
            assertFilterMatches(store, query, techRecent, Set.of("a", "b", "g"));

            // 3) (category == animals) OR (rating > 4.85)
            Filter animalsOrHighRating = Filter.or(
                MetadataFilterBuilder.metadataKey("category").isEqualTo("animals"),
                MetadataFilterBuilder.metadataKey("rating").isGreaterThan(4.85d)
            );
            assertFilterMatches(store, query, animalsOrHighRating, Set.of("c", "g"));

            // 4) (category IN [tech, science]) AND NOT(year < 2020)
            Filter techOrScienceNotOld = Filter.and(
                MetadataFilterBuilder.metadataKey("category").isIn("tech", "science"),
                Filter.not(MetadataFilterBuilder.metadataKey("year").isLessThan(2020))
            );
            assertFilterMatches(store, query, techOrScienceNotOld, Set.of("a", "b", "d", "g"));

            // 5) category NOT IN [tech, science, animals, misc]
            Filter notKnownCategories = MetadataFilterBuilder.metadataKey("category")
                .isNotIn("tech", "science", "animals", "misc");
            assertFilterMatches(store, query, notKnownCategories, Set.of("h"));

            // 6) missing key should match nothing
            Filter missingKey = MetadataFilterBuilder.metadataKey("does_not_exist").isEqualTo("x");
            assertFilterMatches(store, query, missingKey, Set.of());

            System.out.println("   Complex LangChain4j filter coverage: ✓");
        }
        finally {
            store.close();
        }
    }

    private static void assertFilterMatches(ProximumEmbeddingStore store,
                                           Embedding query,
                                           Filter filter,
                                           Set<String> expectedIds) {
        EmbeddingSearchRequest req = EmbeddingSearchRequest.builder()
            .queryEmbedding(query)
            .filter(filter)
            .maxResults(100)
            .build();
        List<EmbeddingMatch<TextSegment>> matches = store.search(req).matches();

        Set<String> got = matches.stream().map(EmbeddingMatch::embeddingId).collect(java.util.stream.Collectors.toSet());
        if (!got.equals(expectedIds)) {
            throw new AssertionError("Filter mismatch. expected=" + expectedIds + " got=" + got);
        }
    }

    private static float[] constantEmbedding(int dims) {
        float[] v = new float[dims];
        for (int i = 0; i < dims; i++) {
            v[i] = 1.0f;
        }
        // normalize
        float norm = (float) Math.sqrt(dims);
        for (int i = 0; i < dims; i++) {
            v[i] /= norm;
        }
        return v;
    }

    private static void deleteRecursively(Path path) throws Exception {
        if (Files.isDirectory(path)) {
            Files.list(path).forEach(p -> {
                try { deleteRecursively(p); } catch (Exception e) { e.printStackTrace(); }
            });
        }
        Files.deleteIfExists(path);
    }
}
