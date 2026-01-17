/**
 * Persistent Vector Database - Embedded vector search with versioning.
 *
 * <h2>Overview</h2>
 * <p>This package provides an embedded vector database optimized for AI/ML applications.
 * Unlike cloud-based solutions, it runs entirely in your JVM with no external dependencies.</p>
 *
 * <h2>Key Features</h2>
 * <ul>
 *   <li><b>Fast HNSW Search</b> - SIMD-optimized approximumimate nearest neighbor search</li>
 *   <li><b>Versioning</b> - Create commits (snapshots) and query historical states</li>
 *   <li><b>Branching</b> - Fork indexes for A/B testing without copying data</li>
 *   <li><b>Durable Persistence</b> - Automatic crash recovery with write-ahead logging</li>
 *   <li><b>Copy-on-Write</b> - O(1) fork operations via structural sharing</li>
 * </ul>
 *
 * <h2>Quick Start</h2>
 * <pre>{@code
 * // Create an index
 * PersistentVectorIndex index = PersistentVectorIndex.builder()
 *     .dimensions(1536)              // OpenAI ada-002
 *     .storagePath("/data/vectors")  // Enable persistence
 *     .distance(DistanceMetric.COSINE)
 *     .build();
 *
 * // Add vectors with metadata
 * float[] embedding = embeddingModel.embed("Hello world");
 * long id = index.add(embedding, Map.of("text", "Hello world", "source", "user"));
 *
 * // Search for similar vectors
 * List<SearchResult> results = index.search(queryEmbedding, 10);
 * for (SearchResult r : results) {
 *     System.out.println("ID: " + r.getId() + ", Similarity: " + r.getSimilarity());
 * }
 *
 * // Persist changes (creates a commit)
 * index.sync();
 * java.util.UUID commitId = index.getCommitId();  // Get commit ID if needed
 *
 * // Close when done
 * index.close();
 * }</pre>
 *
 * <h2>Common Embedding Dimensions</h2>
 * <table>
 *   <tr><th>Model</th><th>Dimensions</th></tr>
 *   <tr><td>OpenAI text-embedding-ada-002</td><td>1536</td></tr>
 *   <tr><td>OpenAI text-embedding-3-small</td><td>1536</td></tr>
 *   <tr><td>OpenAI text-embedding-3-large</td><td>3072</td></tr>
 *   <tr><td>Cohere embed-english-v3.0</td><td>1024</td></tr>
 *   <tr><td>Sentence-BERT (all-MiniLM-L6-v2)</td><td>384</td></tr>
 *   <tr><td>BGE-large-en</td><td>1024</td></tr>
 * </table>
 *
 * <h2>Framework Integration</h2>
 * <p>For Spring AI, see {@link org.replikativ.proximum.spring.PersistentVectorStore}.</p>
 * <p>For LangChain4j, see {@link org.replikativ.proximum.langchain4j.PersistentEmbeddingStore}.</p>
 *
 * <h2>Performance Tuning</h2>
 * <ul>
 *   <li><b>M (16-64)</b>: Higher = better recall, more memory. 16 is good default.</li>
 *   <li><b>efConstruction (100-400)</b>: Higher = better index quality, slower build.</li>
 *   <li><b>efSearch (50-200)</b>: Higher = better recall, slower queries. Set per-query.</li>
 *   <li><b>capacity</b>: Pre-allocate for expected size to avoid resizing.</li>
 * </ul>
 *
 * @see org.replikativ.proximum.PersistentVectorIndex
 * @see org.replikativ.proximum.SearchResult
 * @see org.replikativ.proximum.DistanceMetric
 */
package org.replikativ.proximum;
