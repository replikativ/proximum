package examples;

import org.replikativ.proximum.*;
import java.util.*;

/**
 * Auditable Index Example for ProximumVectorStore
 *
 * This example shows:
 * - Creating an index with crypto-hash enabled for auditability
 * - Getting commit hashes that chain like git
 * - Verifying index integrity from cold storage
 * - Use cases: compliance, backup verification, supply chain integrity
 *
 * Compile and run:
 *   clj -M:dev  # Start with Clojure runtime
 *   # Then from Java:
 *   javac -cp "$(clj -Spath)" examples/java/AuditableIndex.java
 *   java -cp "$(clj -Spath):examples/java" examples.AuditableIndex
 */
public class AuditableIndex {

    public static void main(String[] args) {
        // 1. Create index with crypto-hash enabled for auditability
        try (ProximumVectorStore index = ProximumVectorStore.builder()
                .dimensions(1536)
                .storagePath("/tmp/auditable-vectors")
                .cryptoHash(true)  // Enable SHA-512 based commit hashing
                .build()) {

            System.out.println("Created auditable index");
            System.out.println("Crypto-hash enabled: " + index.isCryptoHash());

            // 2. Add initial vectors and sync
            System.out.println("\n--- First Commit ---");
            for (int i = 0; i < 3; i++) {
                float[] embedding = getEmbedding("Document " + i);
                index.add(embedding, "doc-" + i);
            }
            index.sync();

            // Get the first commit hash
            UUID hash1 = index.getCommitHash();
            System.out.println("First commit hash: " + hash1);
            System.out.println("Vector count: " + index.count());

            // 3. Add more vectors and sync again
            System.out.println("\n--- Second Commit ---");
            for (int i = 3; i < 6; i++) {
                float[] embedding = getEmbedding("Document " + i);
                index.add(embedding, "doc-" + i);
            }
            index.sync();

            // Get the second commit hash - should be different (chains from first)
            UUID hash2 = index.getCommitHash();
            System.out.println("Second commit hash: " + hash2);
            System.out.println("Vector count: " + index.count());
            System.out.println("Hashes differ (chaining): " + !hash1.equals(hash2));

            // 4. Get history of all commits
            System.out.println("\n--- Commit History ---");
            List<Map<String, Object>> history = index.getHistory();
            for (int i = 0; i < Math.min(3, history.size()); i++) {
                Map<String, Object> commit = history.get(i);
                System.out.println("Commit " + i + ":");
                System.out.println("  ID: " + commit.get("proximum/commit-id"));
                System.out.println("  Date: " + commit.get("proximum/created-at"));
            }

            index.close();

            // 5. Verify index integrity from cold storage
            // This reads chunks from disk, recomputes hashes, and verifies integrity
            System.out.println("\n--- Cold Storage Verification ---");
            Map<String, Object> storeConfig = Map.of(
                "backend", ":file",
                "path", "/tmp/auditable-vectors"
            );
            Map<String, Object> verification = ProximumVectorStore.verifyFromCold(storeConfig);
            System.out.println("Verification result:");
            System.out.println("  Valid: " + verification.get("valid?"));
            System.out.println("  Vectors verified: " + verification.get("vectors-verified"));
            System.out.println("  Edges verified: " + verification.get("edges-verified"));
            if (verification.get("expected-hash") != null) {
                System.out.println("  Expected hash: " + verification.get("expected-hash"));
            }

            // Use case examples
            System.out.println("\n--- Use Cases ---");
            System.out.println("1. COMPLIANCE: Commit hashes serve as audit trail for HIPAA/GDPR");
            System.out.println("2. BACKUP VERIFICATION: Detect corruption or tampering during storage");
            System.out.println("3. SUPPLY CHAIN: Verify data integrity when sharing between systems");
            System.out.println("4. REPRODUCIBILITY: Exact hash ensures model can be replicated");

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
        return embedding;
    }
}
