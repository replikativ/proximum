package examples;

import org.replikativ.proximum.*;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Quick Start Example for ProximumVectorStore
 *
 * This example shows:
 * - Creating an index for OpenAI embeddings (1536 dimensions)
 * - Adding vectors with IDs
 * - Batch adding vectors
 * - Searching for similar vectors
 * - Using branching for experiments
 *
 * Compile and run:
 *   clj -M:dev  # Start with Clojure runtime
 *   # Then from Java:
 *   javac -cp "$(clj -Spath)" examples/java/QuickStart.java
 *   java -cp "$(clj -Spath):examples/java" examples.QuickStart
 */
public class QuickStart {

    public static void main(String[] args) {
        // 1. Create an index for OpenAI ada-002 embeddings
        try (ProximumVectorStore index = ProximumVectorStore.builder()
                .dimensions(1536)          // OpenAI ada-002 embedding size
                .storagePath("/tmp/my-vectors")
                .m(16)                      // HNSW neighbors per node
                .efConstruction(200)        // Build quality (higher = better recall)
                .distance(DistanceMetric.COSINE)
                .build()) {

            // 2. Add a vector with an explicit ID
            float[] docEmbedding = getEmbedding("The quick brown fox jumps over the lazy dog");
            String docId = "doc-1";
            index.add(docEmbedding, docId);
            System.out.println("Added document with ID: " + docId);

            // 3. Add a vector with auto-generated UUID (pass null as ID)
            float[] doc2Embedding = getEmbedding("Machine learning is transforming industries");
            index.add(doc2Embedding, null);  // Auto-generates UUID
            System.out.println("Added document with auto-generated UUID");

            // 4. Add a batch of vectors
            List<float[]> batchVectors = List.of(
                getEmbedding("Vector databases enable semantic search"),
                getEmbedding("Embeddings capture meaning in high-dimensional space"),
                getEmbedding("HNSW graphs provide fast approximate search")
            );
            // Provide IDs for batch - use null for auto-generated UUIDs
            List<Object> batchIds = List.of("doc-3", "doc-4", null);
            index.addBatch(batchVectors, batchIds);
            System.out.println("Added batch of " + batchVectors.size() + " vectors");

            // 5. Search for similar documents
            float[] queryEmbedding = getEmbedding("AI is changing how we search");
            List<SearchResult> results = index.search(queryEmbedding, 5);

            System.out.println("\nTop 5 similar documents:");
            for (SearchResult r : results) {
                System.out.printf("  ID: %s, Distance: %.4f, Similarity: %.3f%n",
                    r.getId(), r.getDistance(), r.getSimilarity());
            }

            // 6. Create a branch for experiments
            ProximumVectorStore experiment = index.branch("my-experiment");

            // Modifications to 'experiment' don't affect 'index'
            experiment.add(getEmbedding("Experimental document"), "exp-doc-1");
            System.out.println("\nMain index count: " + index.count());
            System.out.println("Experiment branch count: " + experiment.count());

            // 7. Persist to disk
            index.sync();
            System.out.println("\nIndex synced to storage");
            System.out.println("Current branch: " + index.getCurrentBranch());

            // 8. Later: reconnect to existing index
            // Map<String, Object> storeConfig = Map.of(
            //     "backend", ":file",
            //     "path", "/tmp/my-vectors"
            // );
            // ProximumVectorStore loaded = ProximumVectorStore.connect(storeConfig);

        } catch (Exception e) {
            System.err.println("Vector DB error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Mock embedding function - replace with actual embedding API call
    private static float[] getEmbedding(String text) {
        float[] embedding = new float[1536];
        int hash = text.hashCode();
        for (int i = 0; i < 1536; i++) {
            embedding[i] = (float) Math.sin(hash + i * 0.01);
        }
        // Normalize for cosine similarity
        float norm = 0.0f;
        for (float v : embedding) {
            norm += v * v;
        }
        norm = (float) Math.sqrt(norm);
        for (int i = 0; i < embedding.length; i++) {
            embedding[i] /= norm;
        }
        return embedding;
    }
}
