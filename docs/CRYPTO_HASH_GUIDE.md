# Cryptographic Auditability Guide

Complete guide to Proximum's crypto-hash feature for index auditability and integrity verification.

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [How It Works](#how-it-works)
4. [Use Cases](#use-cases)
5. [API Reference](#api-reference)
6. [Verification from Cold Storage](#verification-from-cold-storage)
7. [Best Practices](#best-practices)
8. [Troubleshooting](#troubleshooting)

---

## Overview

Proximum's crypto-hash feature enables **cryptographic auditability** for vector indices. When enabled, each sync operation computes a commit hash that:

- **Chains with previous commits** (like git)
- **Proves data integrity** after storage or transmission
- **Detects corruption or tampering**
- **Enables backup verification** before restore

This is useful for compliance (HIPAA, GDPR), supply chain integrity, and ensuring reproducibility in ML systems.

### Key Benefits

✅ **Audit Trail**: Every change tracked with cryptographic commit hash
✅ **Tamper Detection**: Detect any unauthorized modifications
✅ **Backup Verification**: Verify backups before restore without loading index
✅ **Reproducibility**: Exact hash ensures model can be replicated
✅ **Supply Chain**: Verify data integrity when sharing between systems

---

## Quick Start

### Java

```java
import org.replikativ.proximum.*;
import java.util.*;

// Create index with crypto-hash enabled
try (ProximumVectorStore store = ProximumVectorStore.builder()
        .dimensions(384)
        .storagePath("/tmp/auditable-vectors")
        .cryptoHash(true)  // Enable auditability
        .build()) {

    // Add vectors and sync
    store.add(embedding1, "doc-1");
    store.add(embedding2, "doc-2");
    store.sync();

    // Get commit hash (unique identifier for this state)
    UUID commitHash = store.getCommitHash();
    System.out.println("Commit hash: " + commitHash);

    // Verify integrity from cold storage
    Map<String, Object> storeConfig = Map.of(
        "backend", ":file",
        "path", "/tmp/auditable-vectors"
    );
    Map<String, Object> result = ProximumVectorStore.verifyFromCold(storeConfig);
    System.out.println("Valid: " + result.get("valid?"));
}
```

### Clojure

```clojure
(require '[proximum.core :as core]
         '[proximum.crypto :as crypto]
         '[proximum.protocols :as p])

;; Create index with crypto-hash enabled
(def idx (core/create-index
           {:type :hnsw
            :dim 384
            :crypto-hash? true  ; Enable auditability
            :store-config {:backend :file :path "/tmp/auditable-vectors"}
            :mmap-dir "/tmp/mmap"}))

;; Add vectors and sync
(def idx (-> idx
             (core/insert embedding1 :doc-1)
             (core/insert embedding2 :doc-2)
             (p/sync!)))

;; Get commit hash
(def commit-hash (crypto/get-commit-hash idx))
(println "Commit hash:" commit-hash)

;; Verify integrity from cold storage
(def result (crypto/verify-from-cold
              {:backend :file :path "/tmp/auditable-vectors"}))
(println "Valid:" (:valid? result))
```

---

## How It Works

### Commit Hash Computation

When crypto-hash is enabled, each `sync()` operation computes a **commit hash** from three components:

```
commit-hash = SHA-512(
  parent-commit-hash +   # Previous commit (git-like chaining)
  vectors-hash +          # Hash of all vector chunks
  edges-hash              # Hash of all HNSW edge chunks
)
```

This creates a **Merkle-tree-like structure** where:
- Each commit chains to its parent (like git)
- Changing any vector or edge changes the commit hash
- The commit hash proves the entire index state

### Deterministic Hashing

**Same input always produces same hash:**

```java
// Two indices with identical data
ProximumVectorStore store1 = builder().cryptoHash(true).build();
ProximumVectorStore store2 = builder().cryptoHash(true).build();

store1.add(vector1, "doc-1");
store1.add(vector2, "doc-2");
store1.sync();

store2.add(vector1, "doc-1");
store2.add(vector2, "doc-2");
store2.sync();

// Commit hashes will be identical
assert store1.getCommitHash().equals(store2.getCommitHash());
```

### Commit Chaining

**Each commit includes the parent hash:**

```java
store.add(vector1, "doc-1");
store.sync();
UUID hash1 = store.getCommitHash();  // First commit

store.add(vector2, "doc-2");
store.sync();
UUID hash2 = store.getCommitHash();  // Second commit (includes hash1 in computation)

// Different hashes due to parent chaining
assert !hash1.equals(hash2);
```

This means:
- You cannot rewrite history (like git)
- Commit hashes prove the entire history
- Tampering with old data changes all subsequent hashes

---

## Use Cases

### 1. Compliance & Audit Trails

For HIPAA, GDPR, or SOX compliance, maintain cryptographic proof of all changes:

```java
// Create auditable patient records index
ProximumVectorStore medicalRecords = ProximumVectorStore.builder()
    .dimensions(768)
    .storagePath("/data/medical-records")
    .cryptoHash(true)  // Required for audit trail
    .build();

// Each update creates auditable commit
medicalRecords.add(patientEmbedding, "patient-123");
medicalRecords.sync();
UUID auditHash = medicalRecords.getCommitHash();

// Log commit hash for compliance
auditLog.record("Updated patient records", auditHash, timestamp);
```

### 2. Backup Verification

Verify backup integrity **before** attempting restore:

```java
// Verify backup without loading entire index
Map<String, Object> storeConfig = Map.of(
    "backend", ":file",
    "path", "/backups/vectors-2024-01-15"
);

Map<String, Object> verification = ProximumVectorStore.verifyFromCold(storeConfig);

if ((Boolean) verification.get("valid?")) {
    // Safe to restore
    System.out.println("Backup verified: " + verification.get("commit-id"));
    System.out.println("Vectors verified: " + verification.get("vectors-verified"));
    System.out.println("Edges verified: " + verification.get("edges-verified"));
} else {
    // Backup corrupted - don't restore
    System.err.println("Backup corrupted: " + verification.get("error"));
}
```

### 3. Supply Chain Integrity

Share vector indices with cryptographic proof of integrity:

```java
// Producer: Create index and share commit hash
ProximumVectorStore store = builder().cryptoHash(true).build();
// ... populate index
store.sync();
UUID commitHash = store.getCommitHash();

// Share: Send both the index files AND the commit hash
shareWithPartner(storeFiles, commitHash);

// Consumer: Verify before using
Map<String, Object> verification = ProximumVectorStore.verifyFromCold(storeConfig);
UUID receivedHash = (UUID) verification.get("commit-id");

if (!receivedHash.equals(expectedCommitHash)) {
    throw new SecurityException("Index tampered during transmission!");
}
```

### 4. Reproducibility in ML Pipelines

Ensure exact vector store state for model reproducibility:

```java
// Training: Record commit hash with model
model.train(data);
vectorStore.sync();
UUID vectorStoreHash = vectorStore.getCommitHash();
model.saveMetadata("vector_store_hash", vectorStoreHash);

// Inference: Verify correct vector store version
UUID currentHash = vectorStore.getCommitHash();
if (!currentHash.equals(model.getVectorStoreHash())) {
    throw new IllegalStateException(
        "Vector store version mismatch - results not reproducible!"
    );
}
```

---

## API Reference

### Java API

#### Enable Crypto-Hash

```java
ProximumVectorStore store = ProximumVectorStore.builder()
    .cryptoHash(true)  // Enable crypto-hash mode
    .build();
```

#### Check if Crypto-Hash Enabled

```java
boolean enabled = store.isCryptoHash();
```

#### Get Current Commit Hash

```java
UUID commitHash = store.getCommitHash();
// Returns null if:
// - crypto-hash is disabled
// - no commits have been made yet (call sync() first)
```

#### Get Commit History

```java
List<Map<String, Object>> history = store.getHistory();
for (Map<String, Object> commit : history) {
    System.out.println("Commit: " + commit.get("proximum/commit-id"));
    System.out.println("Date: " + commit.get("proximum/created-at"));
    System.out.println("Vectors: " + commit.get("proximum/vector-count"));
}
```

#### Verify from Cold Storage

```java
Map<String, Object> storeConfig = Map.of(
    "backend", ":file",
    "path", "/path/to/storage"
);

Map<String, Object> result = ProximumVectorStore.verifyFromCold(storeConfig);

// Check result
Boolean valid = (Boolean) result.get("valid?");
Integer vectorsVerified = (Integer) result.get("vectors-verified");
Integer edgesVerified = (Integer) result.get("edges-verified");
UUID commitId = (UUID) result.get("commit-id");
String error = (String) result.get("error");  // If invalid
```

### Clojure API

#### Enable Crypto-Hash

```clojure
(def idx (core/create-index
           {:crypto-hash? true  ; Enable crypto-hash mode
            ...}))
```

#### Check if Crypto-Hash Enabled

```clojure
(crypto/crypto-hash? idx)  ; => true/false
```

#### Get Current Commit Hash

```clojure
(crypto/get-commit-hash idx)
;; Returns UUID or nil if:
;; - crypto-hash is disabled
;; - no commits have been made yet (call sync! first)
```

#### Get Commit History

```clojure
(p/history idx)
;; Returns vector of commit maps:
;; [{:proximum/commit-id #uuid "..."
;;   :proximum/created-at #inst "..."
;;   :proximum/vector-count 100
;;   ...}
;;  ...]
```

#### Verify from Cold Storage

```clojure
(crypto/verify-from-cold
  {:backend :file :path "/path/to/storage"}
  :main  ; branch (optional, defaults to :main)
  )

;; Returns:
;; {:valid? true
;;  :vectors-verified 10
;;  :edges-verified 5
;;  :commit-id #uuid "..."}
;;
;; Or if invalid:
;; {:valid? false
;;  :error :vectors-invalid
;;  :vectors-result {...}}
```

---

## Verification from Cold Storage

The `verifyFromCold()` operation enables **offline verification** of index integrity without loading the entire index into memory.

### What It Does

1. **Reads all chunks** from Konserve storage (vectors and edges)
2. **Recomputes hashes** for each chunk
3. **Verifies the commit chain** matches expected hashes
4. **Returns detailed results** without loading index

### When to Use

- **Before restoring from backup**: Verify backup integrity
- **After network transfer**: Verify no corruption during transmission
- **Compliance audits**: Prove data integrity without accessing data
- **Supply chain**: Verify received indices before use

### Performance

Verification reads all chunks from storage but:
- ✅ Does not load vectors into memory
- ✅ Does not build HNSW graph
- ✅ Uses streaming chunk verification
- ✅ Suitable for large indices

Typical performance: ~1-2 seconds per GB of index data.

### Example: Automated Backup Verification

```java
public class BackupVerifier {
    public void verifyBackups(String backupDir) {
        File[] backups = new File(backupDir).listFiles();

        for (File backup : backups) {
            System.out.println("Verifying " + backup.getName());

            Map<String, Object> config = Map.of(
                "backend", ":file",
                "path", backup.getAbsolutePath()
            );

            try {
                Map<String, Object> result =
                    ProximumVectorStore.verifyFromCold(config);

                if ((Boolean) result.get("valid?")) {
                    System.out.println("✓ Valid: " + result.get("commit-id"));
                    logBackupStatus(backup, "VALID", result);
                } else {
                    System.err.println("✗ Invalid: " + result.get("error"));
                    logBackupStatus(backup, "CORRUPTED", result);
                    alertOps("Corrupted backup: " + backup.getName());
                }
            } catch (Exception e) {
                System.err.println("✗ Error: " + e.getMessage());
                logBackupStatus(backup, "ERROR", null);
            }
        }
    }
}
```

---

## Best Practices

### 1. Always Enable for Production

Enable crypto-hash for production indices where data integrity matters:

```java
// Development: crypto-hash optional
ProximumVectorStore devStore = builder()
    .cryptoHash(false)  // Faster for development
    .build();

// Production: crypto-hash required
ProximumVectorStore prodStore = builder()
    .cryptoHash(true)   // ALWAYS enable in production
    .build();
```

### 2. Store Commit Hashes Externally

Log commit hashes to external audit log for tamper-proof records:

```java
store.sync();
UUID commitHash = store.getCommitHash();

// Log to tamper-proof external system
auditLog.record(new AuditEntry(
    timestamp: Instant.now(),
    operation: "VECTOR_UPDATE",
    commitHash: commitHash,
    userId: currentUser.getId()
));
```

### 3. Verify Backups Periodically

Schedule regular verification of backups:

```bash
# Cron job: Verify all backups daily
0 2 * * * /usr/local/bin/verify-vector-backups.sh
```

```java
// verify-vector-backups.sh calls:
public class BackupVerificationJob {
    public void run() {
        List<String> backupPaths = listBackups();
        for (String path : backupPaths) {
            verifyBackup(path);
        }
    }
}
```

### 4. Document Commit Hashes in ML Pipelines

Include commit hashes in model metadata:

```python
# Training
vector_store.sync()
commit_hash = vector_store.get_commit_hash()

model_metadata = {
    "model_version": "1.2.0",
    "training_date": "2024-01-15",
    "vector_store_commit": str(commit_hash),  # Required for reproducibility
    "accuracy": 0.95
}
save_model(model, model_metadata)
```

### 5. Performance Considerations

Crypto-hash adds minimal overhead:
- **Insert**: No overhead
- **Search**: No overhead
- **Sync**: ~5-10ms additional time for hash computation

Only disable crypto-hash if:
- Development/testing environment
- Extreme performance requirements (>100k inserts/sec)
- Data integrity not critical

---

## Troubleshooting

### Commit Hash is Null

**Problem**: `getCommitHash()` returns `null`

**Causes**:
1. Crypto-hash not enabled: Check `.cryptoHash(true)` in builder
2. No sync yet: Call `sync()` before getting commit hash
3. Empty index: Add vectors before syncing

**Solution**:
```java
// Ensure crypto-hash enabled
ProximumVectorStore store = builder()
    .cryptoHash(true)  // Must be enabled
    .build();

// Add vectors
store.add(vector, "id");

// Sync to compute hash
store.sync();

// Now hash is available
UUID hash = store.getCommitHash();
assert hash != null;
```

### Verification Fails

**Problem**: `verifyFromCold()` returns `valid? = false`

**Causes**:
1. **Corruption**: Storage corrupted during transmission or backup
2. **Tampering**: Someone modified storage files
3. **Incomplete sync**: Process killed during sync operation
4. **Storage backend mismatch**: Wrong backend type or path

**Solution**:
```java
Map<String, Object> result = ProximumVectorStore.verifyFromCold(config);

if (!(Boolean) result.get("valid?")) {
    String error = (String) result.get("error");

    switch (error) {
        case "branch-not-found":
            // Wrong branch name
            System.err.println("Branch not found: " + result.get("branch"));
            break;

        case "vectors-invalid":
            // Vector chunks corrupted
            System.err.println("Vector corruption detected");
            restoreFromBackup();
            break;

        case "edges-invalid":
            // Edge chunks corrupted
            System.err.println("HNSW graph corruption detected");
            restoreFromBackup();
            break;
    }
}
```

### Different Hashes for Same Data

**Problem**: Two indices with identical data have different commit hashes

**Cause**: Insert order or timing differences

**Explanation**: Commit hashes include parent hash (git-like chaining), so:
- Different insert order → different HNSW graph → different hash
- Different sync timing → different parent chain → different hash

This is **expected behavior** - commit hashes represent the entire history, not just current state.

### Performance Impact

**Problem**: Sync is slower with crypto-hash enabled

**Expected behavior**: Crypto-hash adds ~5-10ms overhead per sync for hash computation.

**If sync is much slower**:
1. Check storage backend performance (network/disk speed)
2. Verify chunk size configuration (larger chunks = fewer hash operations)
3. Consider batching updates before sync

**Mitigation**:
```java
// Batch updates to reduce sync frequency
for (int i = 0; i < 1000; i++) {
    store.add(vectors[i], ids[i]);
}
// Single sync instead of 1000
store.sync();  // Only pays crypto-hash cost once
```

---

## Related Documentation

- [Java Guide](JAVA_GUIDE.md) - General Java API documentation
- [Clojure Guide](CLOJURE_GUIDE.md) - Clojure API documentation
- [Spring AI Guide](SPRING_AI_GUIDE.md) - Spring AI integration
- [LangChain4j Guide](LANGCHAIN4J_GUIDE.md) - LangChain4j integration

## Example Code

See `examples/java/AuditableIndex.java` for a complete working example.
