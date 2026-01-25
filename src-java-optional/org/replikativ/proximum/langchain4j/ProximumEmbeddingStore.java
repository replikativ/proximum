package org.replikativ.proximum.langchain4j;

import org.replikativ.proximum.SearchResult;
import org.replikativ.proximum.DistanceMetric;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.filter.Filter;

import java.util.*;

/**
 * LangChain4j EmbeddingStore implementation backed by Proximum.
 *
 * <p>This adapter provides full LangChain4j compatibility, enabling use with
 * LangChain4j's AI Services, RAG, and memory features.</p>
 *
 * <h2>Features:</h2>
 * <ul>
 *   <li>Full EmbeddingStore interface implementation</li>
 *   <li>TextSegment storage with metadata</li>
 *   <li>Filter support for metadata queries</li>
 *   <li>Versioning and branching (unique to Proximum)</li>
 *   <li>Durable persistence with crash recovery</li>
 * </ul>
 *
 * <h2>Usage:</h2>
 * <pre>{@code
 * EmbeddingStore<TextSegment> store = ProximumEmbeddingStore.builder()
 *     .storagePath("/var/data/embeddings")
 *     .dimensions(1536)
 *     .build();
 *
 * // Add embeddings
 * Embedding embedding = embeddingModel.embed("Hello world").content();
 * TextSegment segment = TextSegment.from("Hello world", Metadata.from("source", "user"));
 * String id = store.add(embedding, segment);
 *
 * // Search
 * EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
 *     .queryEmbedding(queryEmbedding)
 *     .maxResults(10)
 *     .build();
 * EmbeddingSearchResult<TextSegment> results = store.search(request);
 * }</pre>
 *
 * <h2>With AI Services:</h2>
 * <pre>{@code
 * ContentRetriever contentRetriever = EmbeddingStoreContentRetriever.builder()
 *     .embeddingStore(store)
 *     .embeddingModel(embeddingModel)
 *     .maxResults(5)
 *     .build();
 *
 * Assistant assistant = AiServices.builder(Assistant.class)
 *     .chatLanguageModel(chatModel)
 *     .contentRetriever(contentRetriever)
 *     .build();
 * }</pre>
 *
 * @see EmbeddingStore
 */
public class ProximumEmbeddingStore implements EmbeddingStore<TextSegment> {

    private org.replikativ.proximum.ProximumVectorStore store;
    private final int dimensions;

    private ProximumEmbeddingStore(org.replikativ.proximum.ProximumVectorStore store, int dimensions) {
        this.store = Objects.requireNonNull(store, "store");
        if (dimensions <= 0) {
            throw new IllegalArgumentException("dimensions must be set and positive");
        }
        this.dimensions = dimensions;
    }

    private ProximumEmbeddingStore(Builder builder) {
        this.dimensions = builder.dimensions;

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

    /**
     * Create a new builder for ProximumEmbeddingStore.
     *
     * @return builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Connect to an existing persistent embedding store.
     *
     * @param storagePath path to an existing store directory
     * @param dimensions embedding vector dimensions used by this store
     * @return connected store
     */
    public static ProximumEmbeddingStore connect(String storagePath, int dimensions) {
        Map<String, Object> storeConfig = new HashMap<>();
        storeConfig.put("backend", ":file");
        storeConfig.put("path", storagePath);
        storeConfig.put("id", java.util.UUID.randomUUID());

        org.replikativ.proximum.ProximumVectorStore coreStore = org.replikativ.proximum.ProximumVectorStore.connect(storeConfig);
        return new ProximumEmbeddingStore(coreStore, dimensions);
    }

    // -------------------------------------------------------------------------
    // EmbeddingStore Implementation
    // -------------------------------------------------------------------------

    @Override
    public String add(Embedding embedding) {
        return add(embedding, null);
    }

    @Override
    public void add(String id, Embedding embedding) {
        addInternal(id, embedding, null);
    }

    @Override
    public String add(Embedding embedding, TextSegment embedded) {
        String id = UUID.randomUUID().toString();
        addInternal(id, embedding, embedded);
        return id;
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings) {
        return addAll(embeddings, null);
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings, List<TextSegment> embedded) {
        List<String> ids = new ArrayList<>(embeddings.size());
        for (int i = 0; i < embeddings.size(); i++) {
            TextSegment segment = embedded != null && i < embedded.size() ? embedded.get(i) : null;
            String id = add(embeddings.get(i), segment);
            ids.add(id);
        }
        return ids;
    }

    public void addAll(List<String> ids, List<Embedding> embeddings, List<TextSegment> embedded) {
        for (int i = 0; i < ids.size(); i++) {
            TextSegment segment = embedded != null && i < embedded.size() ? embedded.get(i) : null;
            addInternal(ids.get(i), embeddings.get(i), segment);
        }
    }

    @Override
    public void remove(String id) {
        if (id == null) return;
        Integer existingId = store.lookupId(id);
        if (existingId != null) {
            store = store.deleteById(id);
        }
    }

    @Override
    public void removeAll(Collection<String> ids) {
        for (String id : ids) {
            remove(id);
        }
    }

    @Override
    public void removeAll(Filter filter) {
        throw new UnsupportedOperationException("removeAll(filter) is not supported without scanning the dataset");
    }

    @Override
    public void removeAll() {
        throw new UnsupportedOperationException("removeAll() is not supported without scanning the dataset");
    }

    @Override
    public EmbeddingSearchResult<TextSegment> search(EmbeddingSearchRequest request) {
        float[] queryVector = request.queryEmbedding().vector();

        int target = request.maxResults();
        Filter filter = request.filter();

        // If no filter is present, just do a normal search.
        if (filter == null) {
            List<SearchResult> results = store.search(queryVector, target);
            return new EmbeddingSearchResult<>(toMatches(results, target, request.minScore(), null));
        }

        // Filtered search with overfetch.
        int[] multipliers = new int[] { 3, 10, 30 };
        List<EmbeddingMatch<TextSegment>> matches = new ArrayList<>(target);

        for (int multiplier : multipliers) {
            matches.clear();

            int candidateK = safeCandidateK(target, multiplier);
            List<SearchResult> candidates = store.search(queryVector, candidateK);
            matches.addAll(toMatches(candidates, target, request.minScore(), filter));

            if (matches.size() >= target) {
                break;
            }
        }

        return new EmbeddingSearchResult<>(matches);
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

    private List<EmbeddingMatch<TextSegment>> toMatches(List<SearchResult> results,
                                                       int target,
                                                       double minScore,
                                                       Filter filter) {
        List<EmbeddingMatch<TextSegment>> matches = new ArrayList<>(Math.min(target, results.size()));
        for (SearchResult r : results) {
            if (matches.size() >= target) {
                break;
            }

            Map<String, Object> storedMeta = store.getMetadataById(r.getId());
            if (storedMeta == null) {
                storedMeta = new HashMap<>();
            }

            // Apply filter if present
            if (filter != null && !matchesFilter(storedMeta, filter)) {
                continue;
            }

            // Apply min score threshold
            double score = r.getSimilarity();
            if (score < minScore) {
                continue;
            }

            String embeddingId = String.valueOf(r.getId());
            TextSegment segment = toTextSegment(storedMeta);

            // Get the stored embedding
            float[] vector = store.getVectorById(r.getId());
            Embedding embedding = vector != null ? Embedding.from(vector) : null;

            matches.add(new EmbeddingMatch<>(
                score,
                embeddingId,
                embedding,
                segment
            ));
        }
        return matches;
    }

    // -------------------------------------------------------------------------
    // ProximumEmbeddingStore-specific Methods
    // -------------------------------------------------------------------------

    /**
     * Sync all data to durable storage.
     */
    public void sync() {
        store = store.sync();
    }

    /**
     * Sync all data to durable storage with a commit message.
     *
     * @param message the commit message
     */
    public void sync(String message) {
        store = store.sync(message);
    }

    /**
     * Get the underlying store for advanced operations.
     *
     * @return the core ProximumimumVectorStore
     */
    public org.replikativ.proximum.ProximumVectorStore getStore() {
        return store;
    }

    /**
     * Get the number of embeddings in the store.
     *
     * @return embedding count
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

    private void addInternal(String id, Embedding embedding, TextSegment segment) {
        float[] vector = embedding.vector();

        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("id is required for persistence-backed EmbeddingStore");
        }

        // Upsert semantics: if the id exists, delete the old vector first.
        Integer existingId = store.lookupId(id);
        if (existingId != null) {
            store = store.deleteById(id);
        }

        Map<String, Object> metadata = new HashMap<>();
        if (segment != null) {
            metadata.put("_text", segment.text());
            if (segment.metadata() != null) {
                segment.metadata().toMap().forEach((k, v) -> metadata.put(k, v));
            }
        }

        store = store.addWithId(vector, id, metadata);
    }

    private static TextSegment toTextSegment(Map<String, Object> storedMeta) {
        if (storedMeta == null) return null;

        String text = storedMeta.get("_text") != null ? String.valueOf(storedMeta.get("_text")) : null;
        if (text == null || text.isBlank()) {
            return null;
        }

        Map<String, Object> meta = new HashMap<>();
        for (Map.Entry<String, Object> entry : storedMeta.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key == null || value == null) {
                continue;
            }

            if ("_text".equals(key) || "external-id".equals(key)) {
                continue;
            }

            Object normalizedValue = normalizeLangChain4jValue(value);
            if (normalizedValue != null) {
                meta.put(key, normalizedValue);
            }
        }

        return TextSegment.from(text, new Metadata(meta));
    }

    private boolean matchesFilter(Map<String, Object> storedMeta, Filter filter) {
        if (filter == null) {
            return true;
        }

        Map<String, Object> normalized = normalizeForLangChain4jMetadata(storedMeta);
        try {
            return filter.test(Metadata.from(normalized));
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> normalizeForLangChain4jMetadata(Map<String, Object> storedMeta) {
        Map<String, Object> normalized = new HashMap<>();
        if (storedMeta == null) {
            return normalized;
        }

        // Clojure maps may have keyword keys - we need to iterate over raw entries
        Map<Object, Object> rawMap = (Map<Object, Object>) (Map<?, ?>) storedMeta;
        for (Map.Entry<Object, Object> entry : rawMap.entrySet()) {
            Object rawKey = entry.getKey();
            Object value = entry.getValue();
            if (rawKey == null || value == null) {
                continue;
            }

            // Convert Clojure keywords to strings
            String key;
            if (rawKey instanceof clojure.lang.Keyword) {
                key = ((clojure.lang.Keyword) rawKey).getName();
            } else {
                key = String.valueOf(rawKey);
            }

            Object normalizedValue = normalizeLangChain4jValue(value);
            if (normalizedValue != null) {
                normalized.put(key, normalizedValue);
            }
        }

        return normalized;
    }

    private static Object normalizeLangChain4jValue(Object value) {
        if (value instanceof String
            || value instanceof UUID
            || value instanceof Integer
            || value instanceof Long
            || value instanceof Float
            || value instanceof Double) {
            return value;
        }

        if (value instanceof Boolean b) {
            return b.toString();
        }

        if (value instanceof Byte || value instanceof Short) {
            return ((Number) value).intValue();
        }

        if (value instanceof Number n) {
            double d = n.doubleValue();
            if (Double.isFinite(d)) {
                return d;
            }
            return String.valueOf(value);
        }

        return String.valueOf(value);
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    /**
     * Builder for ProximumEmbeddingStore.
     */
    public static class Builder {
        private String storagePath;
        private int dimensions;
        private int m = 16;
        private int efConstruction = 200;
        private int efSearch = 50;
        private int capacity = 10_000_000;
        private DistanceMetric distance = DistanceMetric.COSINE;

        /**
         * Set the storage path (required for persistence).
         */
        public Builder storagePath(String storagePath) {
            this.storagePath = storagePath;
            return this;
        }

        /**
         * Set the embedding dimensions (required).
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
         * Build the ProximumEmbeddingStore.
         */
        public ProximumEmbeddingStore build() {
            if (dimensions <= 0) {
                throw new IllegalArgumentException("dimensions must be set and positive");
            }
            return new ProximumEmbeddingStore(this);
        }
    }
}
