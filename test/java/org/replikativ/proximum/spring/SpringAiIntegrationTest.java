package org.replikativ.proximum.spring;

import org.replikativ.proximum.DistanceMetric;
import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.Embedding;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingRequest;
import org.springframework.ai.embedding.EmbeddingResponse;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.ai.vectorstore.filter.FilterExpressionBuilder;
import org.springframework.ai.vectorstore.filter.Filter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Integration test for Spring AI VectorStore adapter.
 *
 * Run with: clj -M:dev:java-integration -e "(do (require 'proximum.core)
 *   (import 'org.replikativ.proximum.spring.SpringAiIntegrationTest)
 *   (org.replikativ.proximum.spring.SpringAiIntegrationTest/main (into-array String [])))"
 */
public class SpringAiIntegrationTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== Spring AI VectorStore Integration Test ===\n");

        // With strict Konserve semantics, create-store must fail if the target directory exists.
        // So we create a temp parent directory and choose a child path that does not yet exist.
        Path tempParent = Files.createTempDirectory("spring-ai-test");
        Path tempDir = tempParent.resolve("store");

        try {
            // Create a mock embedding model for testing
            EmbeddingModel mockModel = new MockEmbeddingModel();

            // Build the VectorStore
            ProximumVectorStore store = ProximumVectorStore.builder()
                .embeddingModel(mockModel)
                .storagePath(tempDir.toString())
                .dimensions(128)
                .distance(DistanceMetric.COSINE)
                .capacity(1000)
                .build();

            System.out.println("1. Created ProximumVectorStore");

            // Test add documents
            Document doc1 = Document.builder()
                .id("doc-1")
                .text("The quick brown fox jumps over the lazy dog")
                .metadata("category", "animals")
                .build();

            Document doc2 = Document.builder()
                .id("doc-2")
                .text("Machine learning is transforming industries")
                .metadata("category", "tech")
                .build();

            Document doc3 = Document.builder()
                .id("doc-3")
                .text("Vector databases enable semantic search")
                .metadata("category", "tech")
                .build();

            store.add(List.of(doc1, doc2, doc3));
            System.out.println("2. Added 3 documents, count: " + store.count());

            // Test similarity search
            SearchRequest request = SearchRequest.builder()
                .query("AI and machine learning")
                .topK(2)
                .build();

            List<Document> results = store.similaritySearch(request);
            System.out.println("3. Search results for 'AI and machine learning':");
            for (Document doc : results) {
                System.out.printf("   - %s: %.3f similarity%n",
                    doc.getId(),
                    doc.getMetadata().get("similarity"));
            }

            // Test filter expression semantics using Spring AI's public expression DSL
            var b = new FilterExpressionBuilder();
            var techOnly = b.eq("category", "tech").build();

            SearchRequest filteredRequest = SearchRequest.builder()
                .query("AI and machine learning")
                .topK(10)
                .filterExpression(techOnly)
                .build();

            List<Document> filtered = store.similaritySearch(filteredRequest);
            System.out.println("3b. Filtered results (category == tech):");
            for (Document doc : filtered) {
                System.out.printf("   - %s: category=%s%n", doc.getId(), doc.getMetadata().get("category"));
                Object cat = doc.getMetadata().get("category");
                if (!"tech".equals(String.valueOf(cat))) {
                    throw new AssertionError("Expected only tech docs, got: " + doc.getId() + " category=" + cat);
                }
            }

            // Complex filter coverage tests
            runComplexFilterCoverageTests(mockModel, tempDir);

            // Test delete
            store.delete(List.of("doc-1"));
            System.out.println("4. Deleted doc-1, count: " + store.count());

            // Test sync (sync returns CompletableFuture)
            store.sync().join();
            System.out.println("5. Synced to storage");

            // Verify VectorStore interface compliance
            assert store instanceof VectorStore : "Must implement VectorStore";
            System.out.println("6. Implements VectorStore interface: ✓");

            System.out.println("\n=== Spring AI Integration Test PASSED ===");

        } finally {
            deleteRecursively(tempParent);
        }
    }

    /**
     * Mock EmbeddingModel for testing without an actual LLM.
     */
    static class MockEmbeddingModel implements EmbeddingModel {

        @Override
        public EmbeddingResponse call(EmbeddingRequest request) {
            List<Embedding> embeddings = request.getInstructions().stream()
                .map(text -> new Embedding(constantEmbedding(128), 0))
                .toList();
            return new EmbeddingResponse(embeddings);
        }

        @Override
        public float[] embed(String text) {
            return constantEmbedding(128);
        }

        @Override
        public float[] embed(Document document) {
            return constantEmbedding(128);
        }

        @Override
        public List<float[]> embed(List<String> texts) {
            return texts.stream().map(t -> constantEmbedding(128)).toList();
        }

        @Override
        public EmbeddingResponse embedForResponse(List<String> texts) {
            List<Embedding> embeddings = texts.stream()
                .map(text -> new Embedding(constantEmbedding(128), 0))
                .toList();
            return new EmbeddingResponse(embeddings);
        }

        @Override
        public int dimensions() {
            return 128;
        }
    }

    private static void runComplexFilterCoverageTests(EmbeddingModel embeddingModel, Path parentTempDir) throws Exception {
        System.out.println("3c. Complex filter coverage (parser + AST, AND/OR/NOT, IN/NIN, numeric/boolean)");

        Path filterDir = parentTempDir.resolve("spring-ai-filters-" + System.currentTimeMillis());
        ProximumVectorStore store = ProximumVectorStore.builder()
            .embeddingModel(embeddingModel)
            .storagePath(filterDir.toString())
            .dimensions(128)
            .distance(DistanceMetric.COSINE)
            .capacity(1000)
            .build();

        try {

            // Add a small deterministic dataset (query embedding is constant, so topK can fetch all)
            Document d1 = Document.builder()
            .id("f-1")
            .text("one")
            .metadata(Map.of(
                "country", "BG",
                "year", 2020,
                "city", "Varna",
                "enabled", true,
                "isOpen", true,
                "temperature", 10.0,
                "biz_id", 3,
                "classification", "typeA",
                "activationDate", new Date(2000)
            ))
            .build();

            Document d2 = Document.builder()
            .id("f-2")
            .text("two")
            .metadata(Map.of(
                "country", "NL",
                "year", 2022,
                "city", "Sofia",
                "enabled", false,
                "isOpen", true,
                "temperature", -20.0,
                "biz_id", 5,
                "classification", "typeB",
                "activationDate", new Date(3000)
            ))
            .build();

            Document d3 = Document.builder()
            .id("f-3")
            .text("three")
            .metadata(Map.of(
                "country", "US",
                "year", 2019,
                "city", "Plovdiv",
                "enabled", true,
                "isOpen", false,
                "temperature", 20.13,
                "biz_id", -5,
                "classification", "typeA",
                "activationDate", new Date(2000)
            ))
            .build();

            store.add(List.of(d1, d2, d3));

        // Helper that forces retrieval of all docs then validates filtered ids.
            java.util.function.BiConsumer<SearchRequest, Set<String>> assertIds = (req, expected) -> {
                List<Document> docs = store.similaritySearch(req);
                Set<String> got = docs.stream().map(Document::getId).collect(Collectors.toSet());
                if (!got.equals(expected)) {
                    throw new AssertionError("Filter mismatch. expected=" + expected + " got=" + got
                        + " expr=" + (req.hasFilterExpression() ? String.valueOf(req.getFilterExpression()) : "<none>"));
                }
            };

        // 1) Parser: country == 'BG' && year >= 2020
            assertIds.accept(SearchRequest.builder().query("x").topK(50)
                .filterExpression("country == 'BG' && year >= 2020")
                .build(), Set.of("f-1"));

        // 2) Parser precedence: year >= 2020 OR country == "BG" AND city != "Sofia"
        // Equivalent to: (year>=2020) OR (country==BG AND city!=Sofia)
            assertIds.accept(SearchRequest.builder().query("x").topK(50)
                .filterExpression("year >= 2020 OR country == \"BG\" AND city != \"Sofia\"")
                .build(), Set.of("f-1", "f-2"));

        // 3) Parser grouping + NIN: (year >= 2020 OR country == "BG") AND city NIN ["Sofia", "Plovdiv"]
            assertIds.accept(SearchRequest.builder().query("x").topK(50)
                .filterExpression("(year >= 2020 OR country == \"BG\") AND city NIN [\"Sofia\", \"Plovdiv\"]")
                .build(), Set.of("f-1"));

        // 4) Parser NOT + IN
            assertIds.accept(SearchRequest.builder().query("x").topK(50)
                .filterExpression("not(isOpen == true AND year >= 2020 AND country IN [\"BG\", \"NL\", \"US\"])")
                .build(), Set.of("f-3"));

        // 5) Nested NOT
            assertIds.accept(SearchRequest.builder().query("x").topK(50)
                .filterExpression("not(isOpen == true AND year >= 2020 AND NOT(country IN [\"BG\"]))")
                .build(), Set.of("f-1", "f-3"));

        // 6) Decimal range
            assertIds.accept(SearchRequest.builder().query("x").topK(50)
                .filterExpression("temperature >= -15.6 && temperature <= +20.13")
                .build(), Set.of("f-1", "f-3"));

        // 7) Integer literal
            assertIds.accept(SearchRequest.builder().query("x").topK(50)
            .filterExpression("biz_id == 3")
                .build(), Set.of("f-1"));

        // 8) Boolean equality
            assertIds.accept(SearchRequest.builder().query("x").topK(50)
                .filterExpression("enabled == true")
                .build(), Set.of("f-1", "f-3"));

        // 9) Not equal
            assertIds.accept(SearchRequest.builder().query("x").topK(50)
                .filterExpression("classification != 'typeB'")
                .build(), Set.of("f-1", "f-3"));

        // 10) AST: ISNULL treats missing keys as null (operator availability depends on Spring AI version)
            Filter.ExpressionType isNullType = expressionTypeOrNull("ISNULL", "IS_NULL");
            if (isNullType != null) {
                Filter.Expression isNullMissing = new Filter.Expression(isNullType, new Filter.Key("missing"), null);
                assertIds.accept(SearchRequest.builder().query("x").topK(50).filterExpression(isNullMissing).build(),
                    Set.of("f-1", "f-2", "f-3"));
            }
            else {
                System.out.println(
                    "   Skipping ISNULL missing-key test (ExpressionType not supported by this Spring AI version)");
            }

        // 11) AST: compare Date values (same-type Comparable)
            Filter.Expression activationIs2000 = new Filter.Expression(Filter.ExpressionType.EQ,
                new Filter.Key("activationDate"), new Filter.Value(new Date(2000)));
            assertIds.accept(SearchRequest.builder().query("x").topK(50).filterExpression(activationIs2000).build(),
                Set.of("f-1", "f-3"));

            System.out.println("   Complex Spring AI filter coverage: ✓");
        }
        finally {
            store.close();
        }
    }

    private static Filter.ExpressionType expressionTypeOrNull(String... candidates) {
        for (String name : candidates) {
            try {
                return Filter.ExpressionType.valueOf(name);
            }
            catch (IllegalArgumentException ignored) {
                // try next
            }
        }
        return null;
    }

    private static float[] constantEmbedding(int dims) {
        float[] v = new float[dims];
        for (int i = 0; i < dims; i++) {
            v[i] = 1.0f;
        }
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
