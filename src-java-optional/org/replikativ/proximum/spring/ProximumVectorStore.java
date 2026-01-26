package org.replikativ.proximum.spring;

import org.replikativ.proximum.SearchResult;
import org.replikativ.proximum.DistanceMetric;

import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.ai.vectorstore.filter.Filter;

import java.util.*;
import java.math.BigDecimal;
import java.util.Optional;

/**
 * Spring AI VectorStore implementation backed by Proximum.
 *
 * <p>This adapter provides full Spring AI compatibility, enabling use with
 * Spring AI's RAG features, ChatClient advisors, and the standard VectorStore API.</p>
 *
 * <h2>Features:</h2>
 * <ul>
 *   <li>Full VectorStore interface implementation</li>
 *   <li>Automatic embedding via EmbeddingModel</li>
 *   <li>Metadata filtering support</li>
 *   <li>Versioning and branching (unique to Proximum)</li>
 *   <li>Durable persistence with crash recovery</li>
 * </ul>
 *
 * <h2>Usage with Spring Boot:</h2>
 * <pre>{@code
 * @Configuration
 * public class VectorStoreConfig {
 *     @Bean
 *     public VectorStore vectorStore(EmbeddingModel embeddingModel) {
 *         return ProximumVectorStore.builder()
 *             .embeddingModel(embeddingModel)
 *             .storagePath("/var/data/vectors")
 *             .dimensions(1536)  // OpenAI ada-002
 *             .build();
 *     }
 * }
 * }</pre>
 *
 * <h2>Usage with RAG:</h2>
 * <pre>{@code
 * @Autowired VectorStore vectorStore;
 * @Autowired ChatClient.Builder chatClientBuilder;
 *
 * ChatClient chatClient = chatClientBuilder
 *     .defaultAdvisors(new QuestionAnswerAdvisor(vectorStore))
 *     .build();
 *
 * String answer = chatClient.prompt("What is the refund policy?").call().content();
 * }</pre>
 *
 * @see VectorStore
 */
public class ProximumVectorStore implements VectorStore {

    private org.replikativ.proximum.ProximumVectorStore store;
    private final EmbeddingModel embeddingModel;
    private final int dimensions;

    private ProximumVectorStore(Builder builder) {
        this.embeddingModel = builder.embeddingModel;
        this.dimensions = builder.dimensions > 0 ? builder.dimensions : detectDimensions(embeddingModel);

        Map<String, Object> storeConfig = new HashMap<>();
        storeConfig.put("backend", ":file");
        storeConfig.put("path", builder.storagePath);
        storeConfig.put("id", java.util.UUID.randomUUID());

        this.store = org.replikativ.proximum.ProximumVectorStore.builder()
            .dim(this.dimensions)
            .storeConfig(storeConfig)
            .m(builder.m)
            .efConstruction(builder.efConstruction)
            .capacity(builder.capacity)
            .distance(builder.distance)
            .build();
    }

    private int detectDimensions(EmbeddingModel model) {
        float[] test = model.embed("test");
        return test.length;
    }

    /**
     * Create a new builder for ProximumVectorStore.
     *
     * @return builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    // -------------------------------------------------------------------------
    // VectorStore Implementation
    // -------------------------------------------------------------------------

    @Override
    public void add(List<Document> documents) {
        for (Document doc : documents) {
            float[] embedding = getOrComputeEmbedding(doc);
            String docId = doc.getId();
            if (docId == null || docId.isEmpty()) {
                throw new IllegalArgumentException("Document id is required for persistence-backed VectorStore");
            }

            // Upsert semantics: if the id exists, delete the old vector first.
            Integer existingId = store.lookupId(docId);
            if (existingId != null) {
                store = store.deleteById(docId);
            }

            Map<String, Object> metadata = new HashMap<>(doc.getMetadata());
            if (doc.getText() != null) {
                metadata.put("_content", doc.getText());
            }

            // Use addWithId to store the document ID as external ID
            store = store.addWithId(embedding, docId, metadata);
        }
    }

    @Override
    public Optional<Boolean> delete(List<String> idList) {
        boolean anyDeleted = false;
        for (String docId : idList) {
            if (docId == null) continue;
            Integer existingId = store.lookupId(docId);
            if (existingId != null) {
                store = store.deleteById(docId);
                anyDeleted = true;
            }
        }
        return Optional.of(anyDeleted);
    }

    @Override
    public List<Document> similaritySearch(SearchRequest request) {
        float[] queryEmbedding = embeddingModel.embed(request.getQuery());

        int target = request.getTopK();
        double minSim = request.getSimilarityThreshold();
        Filter.Expression filter = request.hasFilterExpression() ? request.getFilterExpression() : null;

        // No filter -> direct search
        if (filter == null) {
            List<SearchResult> results = store.search(queryEmbedding, target);
            return toDocuments(results, target, minSim, null);
        }

        // Post-hoc filtering with bounded overfetch.
        int[] multipliers = new int[] { 3, 10, 30 };
        List<Document> docs = new ArrayList<>(target);
        for (int multiplier : multipliers) {
            docs.clear();

            int candidateK = safeCandidateK(target, multiplier);
            List<SearchResult> candidates = store.search(queryEmbedding, candidateK);
            docs.addAll(toDocuments(candidates, target, minSim, filter));
            if (docs.size() >= target) {
                break;
            }
        }

        return docs;
    }

    private static int safeCandidateK(int target, int multiplier) {
        if (target <= 0) {
            return 0;
        }
        long k = (long) target * (long) multiplier;
        if (k > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) k;
    }

    private List<Document> toDocuments(List<SearchResult> results,
                                      int target,
                                      double minSim,
                                      Filter.Expression filter) {
        List<Document> documents = new ArrayList<>(Math.min(target, results.size()));
        for (SearchResult r : results) {
            if (documents.size() >= target) {
                break;
            }

            // Apply similarity threshold filter
            if (minSim > 0 && r.getSimilarity() < minSim) {
                continue;
            }

            Map<String, Object> storedMeta = normalizeClojureMap(store.getMetadataById(r.getId()));
            if (storedMeta == null) {
                storedMeta = new HashMap<>();
            }

            if (filter != null && !matchesFilter(storedMeta, filter)) {
                continue;
            }

            String id = String.valueOf(r.getId());

            String content = storedMeta.get("_content") != null
                ? String.valueOf(storedMeta.get("_content"))
                : null;

            // Create new document with distance metadata
            Map<String, Object> outMeta = new HashMap<>(storedMeta);
            outMeta.remove("_content");
            outMeta.put("distance", r.getDistance());
            outMeta.put("similarity", r.getSimilarity());

            documents.add(Document.builder()
                .id(id)
                .text(content)
                .metadata(outMeta)
                .build());
        }

        return documents;
    }

    // -------------------------------------------------------------------------
    // ProximumVectorStore-specific Methods
    // -------------------------------------------------------------------------

    /**
     * Sync all data to durable storage.
     * Blocks until sync completes.
     */
    public void sync() {
        try {
            store = store.sync().get();
        } catch (Exception e) {
            throw new RuntimeException("Sync failed", e);
        }
    }

    /**
     * Sync all data to durable storage with a commit message.
     * Blocks until sync completes.
     *
     * @param message the commit message
     */
    public void sync(String message) {
        try {
            Map<String, Object> opts = new HashMap<>();
            opts.put(":message", message);
            store = store.sync(opts).get();
        } catch (Exception e) {
            throw new RuntimeException("Sync failed", e);
        }
    }

    /**
     * Get the underlying store for advanced operations.
     *
     * @return the core ProximumVectorStore
     */
    public org.replikativ.proximum.ProximumVectorStore getStore() {
        return store;
    }

    /**
     * Get the number of documents in the store.
     *
     * @return document count
     */
    public long count() {
        return store.count();
    }

    /**
     * Close the store and release resources.
     */
    public void close() {
        store.close();
    }

    // -------------------------------------------------------------------------
    // Helper Methods
    // -------------------------------------------------------------------------

    private float[] getOrComputeEmbedding(Document doc) {
        Object existingEmbedding = doc.getMetadata().get("embedding");
        if (existingEmbedding instanceof float[]) {
            return (float[]) existingEmbedding;
        }

        String text = doc.getText();
        if (text == null || text.isEmpty()) {
            throw new IllegalArgumentException("Document must have text content for embedding");
        }

        return embeddingModel.embed(text);
    }

    /**
     * Normalize a Clojure map (with keyword keys) to a Java map with String keys.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> normalizeClojureMap(Map<String, Object> clojureMap) {
        if (clojureMap == null) {
            return null;
        }
        Map<String, Object> result = new HashMap<>();
        Map<Object, Object> rawMap = (Map<Object, Object>) (Map<?, ?>) clojureMap;
        for (Map.Entry<Object, Object> entry : rawMap.entrySet()) {
            Object rawKey = entry.getKey();
            if (rawKey == null) continue;

            String key;
            if (rawKey instanceof clojure.lang.Keyword) {
                key = ((clojure.lang.Keyword) rawKey).getName();
            } else {
                key = String.valueOf(rawKey);
            }
            result.put(key, entry.getValue());
        }
        return result;
    }

    private boolean matchesFilter(Map<String, Object> storedMeta, Filter.Expression filter) {
        if (filter == null) {
            return true;
        }

        return evaluateFilter(storedMeta, filter);
    }


    private static boolean evaluateFilter(Map<String, Object> attrs, Filter.Operand operand) {
        if (operand == null) {
            return true;
        }

        if (operand instanceof Filter.Group group) {
            return evaluateFilter(attrs, group.content());
        }

        if (!(operand instanceof Filter.Expression expr)) {
            // Unknown operand type; fail closed.
            return false;
        }

        String type = expr.type().name();
        return switch (type) {
            case "AND" -> evaluateFilter(attrs, expr.left()) && evaluateFilter(attrs, expr.right());
            case "OR" -> evaluateFilter(attrs, expr.left()) || evaluateFilter(attrs, expr.right());
            case "NOT" -> !evaluateFilter(attrs, expr.left());
            case "ISNULL", "IS_NULL" -> isNull(attrs, expr.left());
            case "ISNOTNULL", "IS_NOT_NULL" -> !isNull(attrs, expr.left());
            case "EQ" -> compare(attrs, expr.left(), expr.right()) == 0;
            case "NE" -> compare(attrs, expr.left(), expr.right()) != 0;
            case "GT" -> compare(attrs, expr.left(), expr.right()) > 0;
            case "GTE" -> compare(attrs, expr.left(), expr.right()) >= 0;
            case "LT" -> compare(attrs, expr.left(), expr.right()) < 0;
            case "LTE" -> compare(attrs, expr.left(), expr.right()) <= 0;
            case "IN" -> in(attrs, expr.left(), expr.right());
            case "NIN" -> !in(attrs, expr.left(), expr.right());
            default -> false;
        };
    }

    private static boolean isNull(Map<String, Object> attrs, Filter.Operand left) {
        if (!(left instanceof Filter.Key key)) {
            return true;
        }
        // Treat missing keys as null.
        if (attrs == null || !attrs.containsKey(key.key())) {
            return true;
        }
        return attrs.get(key.key()) == null;
    }

    private static int compare(Map<String, Object> attrs, Filter.Operand left, Filter.Operand right) {
        Object actual = readKey(attrs, left);
        Object expected = readValue(right);

        if (actual == null && expected == null) {
            return 0;
        }
        if (actual == null || expected == null) {
            return -1;
        }

        // Numeric comparisons via BigDecimal
        Optional<BigDecimal> aNum = toBigDecimal(actual);
        Optional<BigDecimal> eNum = toBigDecimal(expected);
        if (aNum.isPresent() && eNum.isPresent()) {
            return aNum.get().compareTo(eNum.get());
        }

        // Boolean equality
        if (actual instanceof Boolean aBool) {
            Boolean eBool = toBoolean(expected);
            if (eBool != null) {
                return Boolean.compare(aBool, eBool);
            }
        }
        if (expected instanceof Boolean eBool) {
            Boolean aBool = toBoolean(actual);
            if (aBool != null) {
                return Boolean.compare(aBool, eBool);
            }
        }

        // Same-type Comparable
        if (actual.getClass().equals(expected.getClass()) && actual instanceof Comparable<?> aComp) {
            @SuppressWarnings("unchecked")
            Comparable<Object> cmp = (Comparable<Object>) aComp;
            return cmp.compareTo(expected);
        }

        // Fallback to string compare for ordering.
        return String.valueOf(actual).compareTo(String.valueOf(expected));
    }

    private static boolean in(Map<String, Object> attrs, Filter.Operand left, Filter.Operand right) {
        Object actual = readKey(attrs, left);
        if (actual == null) {
            return false;
        }

        Object expected = readValue(right);
        if (expected instanceof Iterable<?> it) {
            for (Object candidate : it) {
                if (equalsLoose(actual, candidate)) {
                    return true;
                }
            }
            return false;
        }

        if (expected != null && expected.getClass().isArray()) {
            int len = java.lang.reflect.Array.getLength(expected);
            for (int i = 0; i < len; i++) {
                Object candidate = java.lang.reflect.Array.get(expected, i);
                if (equalsLoose(actual, candidate)) {
                    return true;
                }
            }
            return false;
        }

        // Treat scalar as singleton set.
        return equalsLoose(actual, expected);
    }

    private static Object readKey(Map<String, Object> attrs, Filter.Operand operand) {
        if (!(operand instanceof Filter.Key key)) {
            return null;
        }
        if (attrs == null) {
            return null;
        }
        return attrs.get(key.key());
    }

    private static Object readValue(Filter.Operand operand) {
        if (operand == null) {
            return null;
        }
        if (operand instanceof Filter.Value v) {
            return v.value();
        }
        return null;
    }

    private static boolean equalsLoose(Object a, Object b) {
        if (Objects.equals(a, b)) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }

        Optional<BigDecimal> aNum = toBigDecimal(a);
        Optional<BigDecimal> bNum = toBigDecimal(b);
        if (aNum.isPresent() && bNum.isPresent()) {
            return aNum.get().compareTo(bNum.get()) == 0;
        }

        Boolean aBool = toBoolean(a);
        Boolean bBool = toBoolean(b);
        if (aBool != null && bBool != null) {
            return aBool.equals(bBool);
        }

        return String.valueOf(a).equals(String.valueOf(b));
    }

    private static Optional<BigDecimal> toBigDecimal(Object o) {
        if (o == null) {
            return Optional.empty();
        }
        if (o instanceof BigDecimal bd) {
            return Optional.of(bd);
        }
        if (o instanceof Number n) {
            try {
                return Optional.of(new BigDecimal(String.valueOf(n)));
            }
            catch (NumberFormatException e) {
                return Optional.empty();
            }
        }
        if (o instanceof String s) {
            try {
                return Optional.of(new BigDecimal(s));
            }
            catch (NumberFormatException e) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private static Boolean toBoolean(Object o) {
        if (o instanceof Boolean b) {
            return b;
        }
        if (o instanceof String s) {
            String norm = s.trim().toLowerCase(Locale.ROOT);
            if ("true".equals(norm)) return true;
            if ("false".equals(norm)) return false;
        }
        return null;
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    /**
     * Builder for ProximumVectorStore.
     */
    public static class Builder {
        private EmbeddingModel embeddingModel;
        private String storagePath;
        private int dimensions = 0;
        private int m = 16;
        private int efConstruction = 200;
        private int efSearch = 50;
        private int capacity = 10_000_000;
        private DistanceMetric distance = DistanceMetric.COSINE;

        /**
         * Set the embedding model (required).
         */
        public Builder embeddingModel(EmbeddingModel embeddingModel) {
            this.embeddingModel = embeddingModel;
            return this;
        }

        /**
         * Set the storage path for persistence.
         */
        public Builder storagePath(String storagePath) {
            this.storagePath = storagePath;
            return this;
        }

        /**
         * Set the embedding dimensions (auto-detected if not set).
         */
        public Builder dimensions(int dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        /**
         * Set the M parameter (neighbors per node). Default: 16.
         */
        public Builder m(int m) {
            this.m = m;
            return this;
        }

        /**
         * Set ef-construction (build quality). Default: 200.
         */
        public Builder efConstruction(int efConstruction) {
            this.efConstruction = efConstruction;
            return this;
        }

        /**
         * Set ef-search (query quality). Default: 50.
         */
        public Builder efSearch(int efSearch) {
            this.efSearch = efSearch;
            return this;
        }

        /**
         * Set maximum capacity. Default: 10,000,000.
         */
        public Builder capacity(int capacity) {
            this.capacity = capacity;
            return this;
        }

        /**
         * Set the distance metric. Default: COSINE.
         */
        public Builder distance(DistanceMetric distance) {
            this.distance = distance;
            return this;
        }

        /**
         * Build the ProximumVectorStore.
         */
        public ProximumVectorStore build() {
            if (embeddingModel == null) {
                throw new IllegalArgumentException("embeddingModel is required");
            }
            return new ProximumVectorStore(this);
        }
    }
}
