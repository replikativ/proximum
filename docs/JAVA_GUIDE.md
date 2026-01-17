# Proximum Java Guide

Complete guide to using Proximum from Java.

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Core Concepts](#core-concepts)
4. [Basic Operations](#basic-operations)
5. [Search & Latency Controls](#search--latency-controls)
6. [Versioning & Branching](#versioning--branching)
7. [Metadata](#metadata)
8. [Advanced Features](#advanced-features)
9. [Spring AI Integration](#spring-ai-integration)
10. [LangChain4j Integration](#langchain4j-integration)
11. [Performance Tips](#performance-tips)
12. [Troubleshooting](#troubleshooting)

---

## Installation

[![Clojars Project](https://img.shields.io/clojars/v/org.replikativ/proximum.svg)](https://clojars.org/org.replikativ/proximum)

### Maven

```xml
<dependency>
  <groupId>org.replikativ</groupId>
  <artifactId>proximum</artifactId>
  <version>LATEST</version>
</dependency>
```

### Gradle

```gradle
implementation 'org.replikativ:proximum:LATEST'
```

### Requirements

- **Java 21+** (for Vector API and Foreign Memory)
- **JVM Options**: Add these to your JVM arguments:
  ```
  --add-modules=jdk.incubator.vector
  --enable-native-access=ALL-UNNAMED
  ```

**Maven Surefire example:**
```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-surefire-plugin</artifactId>
  <configuration>
    <argLine>--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED</argLine>
  </configuration>
</plugin>
```

---

## Quick Start

```java
import org.replikativ.proximum.*;
import java.util.*;

public class QuickStart {
    public static void main(String[] args) {
        // Create index with builder pattern
        try (ProximumVectorStore store = ProximumVectorStore.builder()
                .dimensions(384)
                .storagePath("/tmp/vectors")
                .build()) {

            // Add vectors (immutable - returns new store)
            store = store.add(embedding1, "doc-1");
            store = store.add(embedding2, "doc-2");

            // Search for nearest neighbors
            List<SearchResult> results = store.search(queryVector, 5);
            for (SearchResult r : results) {
                System.out.println(r.getId() + ": " + r.getDistance());
            }

            // Persist to storage
            store.sync();
        }
    }
}
```

That's it! Proximum provides a clean Java API with immutable semantics.

---

## Core Concepts

### Immutable Semantics

Unlike traditional databases, Proximum follows **immutable/persistent data structure patterns**:

```java
ProximumVectorStore store1 = builder.build();
ProximumVectorStore store2 = store1.add(vector1, "doc-1");
ProximumVectorStore store3 = store2.add(vector2, "doc-2");

// store1, store2, store3 are all valid and queryable!
System.out.println(store1.count());  // => 0
System.out.println(store2.count());  // => 1
System.out.println(store3.count());  // => 2
```

**Key properties:**

- ✅ **Immutable**: Operations return new instances, originals unchanged
- ✅ **Structural Sharing**: Shared data between versions (copy-on-write)
- ✅ **Snapshot Semantics**: Each version is independently queryable
- ✅ **Thread-Safe**: No locks needed for concurrent reads

**Pattern:**
```java
// Reassign to variable (like final var in modern Java)
store = store.add(vector, id);
store = store.add(vector2, id2);
store.sync();
```

### Git-Like Versioning

Proximum provides git-like version control:

```java
// Create a commit (like git commit)
store.sync();
UUID commitId = store.getCommitId();

// Create a branch (like git branch)
ProximumVectorStore experiment = store.branch("experiment");

// Modifications to experiment don't affect store
experiment = experiment.add(newVector, "exp-doc");

System.out.println(store.count());      // => 2
System.out.println(experiment.count()); // => 3
```

**Use cases:**
- **A/B Testing**: Test new embedding models on a branch
- **Staging**: Validate changes before promoting to production
- **Compliance**: Maintain audit trails with time-travel queries
- **Debugging**: Reproduce exact historical state

### When to Sync

Understanding when to persist is critical:

```java
// In-memory operations (fast)
store = store.add(vec1, "a");      // Instant
store = store.add(vec2, "b");      // Instant
store.search(query, 10);           // Instant

// Persist to disk (slower)
store.sync();                      // Writes to storage

// After JVM restart, data is gone unless you sync!
```

**Rules:**
- **Development**: Sync rarely, use in-memory indices
- **Production**: Sync after batches, on shutdown, periodically
- **Experiments**: Create branches instead of separate indices

---

## Basic Operations

### Creating an Index

**In-Memory Index** (for testing):

```java
ProximumVectorStore store = ProximumVectorStore.builder()
    .dimensions(384)
    .build();  // No storage path = in-memory only
```

**Persistent Index** (for production):

```java
ProximumVectorStore store = ProximumVectorStore.builder()
    .dimensions(1536)                    // OpenAI ada-002 size
    .storagePath("/var/data/vectors")   // File backend
    .m(16)                               // Max neighbors per node
    .efConstruction(200)                 // Build quality
    .distance(DistanceMetric.COSINE)    // Metric for normalized vectors
    .capacity(1_000_000)                 // Expected max vectors
    .build();
```

**Configuration Options:**

| Method | Description | Default | Recommendation |
|--------|-------------|---------|----------------|
| `dimensions(int)` | Vector dimensions | Required | Match your embeddings |
| `m(int)` | Max neighbors per node | 16 | 16-32 for most use cases |
| `efConstruction(int)` | Build quality | 200 | 100-400 (higher = better recall) |
| `efSearch(int)` | Search quality | 50 | 50-200 (override per-query) |
| `capacity(int)` | Max vectors | 10M | Set to expected size |
| `distance(DistanceMetric)` | Metric | EUCLIDEAN | COSINE for normalized |

### Inserting Vectors

**Single Insert:**

```java
float[] embedding = getEmbedding("My document");
store = store.add(embedding, "doc-1");

// Auto-generate UUID
store = store.add(embedding, null);  // Returns UUID in search results
```

**Batch Insert:**

```java
List<float[]> vectors = List.of(
    getEmbedding("Document 1"),
    getEmbedding("Document 2"),
    getEmbedding("Document 3")
);
List<Object> ids = List.of("doc-1", "doc-2", "doc-3");

store = store.addBatch(vectors, ids);
```

**With Metadata:**

```java
// Add vector first
store = store.add(embedding, "doc-1");

// Then add metadata
Map<String, Object> metadata = Map.of(
    "title", "Introduction to AI",
    "category", "ai",
    "timestamp", System.currentTimeMillis()
);
store = store.withMetadata("doc-1", metadata);
```

### Searching

**Basic Search:**

```java
float[] query = getEmbedding("machine learning");
List<SearchResult> results = store.search(query, 10);

for (SearchResult r : results) {
    System.out.printf("ID: %s, Distance: %.4f, Similarity: %.3f%n",
        r.getId(), r.getDistance(), r.getSimilarity());
}
```

**With Search Options:**

```java
Map<String, Object> options = Map.of(
    "ef", 100,          // Higher ef = better recall
    "timeout-ms", 50    // Timeout in milliseconds
);
List<SearchResult> results = store.searchWithIds(query, 10, options);
```

### Retrieving Vectors

```java
float[] vector = store.getVector("doc-1");
if (vector != null) {
    System.out.println("Found vector with " + vector.length + " dimensions");
}

// Get metadata
Map<String, Object> metadata = store.getMetadata("doc-1");
```

### Deleting Vectors

**Soft Delete:**

```java
store = store.delete("doc-1");

// Vector marked deleted but space not reclaimed
Map<String, Object> metrics = store.getMetrics();
System.out.println("Deletion ratio: " + metrics.get("deletion-ratio"));
```

**Compaction (Reclaim Space):**

```java
// Check if compaction needed
if (store.isNeedsCompaction()) {
    Map<String, Object> target = Map.of(
        "store-config", Map.of(
            "backend", ":file",
            "path", "/var/data/vectors-new",
            "id", UUID.randomUUID()
        ),
        "mmap-dir", "/var/data/mmap-new"
    );

    ProximumVectorStore compacted = store.compact(target);
    // Use compacted store, old one can be discarded
}
```

---

## Search & Latency Controls

### Basic Search

```java
List<SearchResult> results = store.search(queryVector, 10);

// Extract information
for (SearchResult r : results) {
    Object id = r.getId();              // External ID
    double distance = r.getDistance();  // Raw distance
    double similarity = r.getSimilarity(); // Normalized 0-1
}
```

### Latency Controls

Control search latency and quality with options:

```java
// Higher ef = better recall, slower search
Map<String, Object> options = Map.of("ef", 100);
List<SearchResult> results = store.searchWithIds(query, 10, options);

// Timeout to prevent slow queries
options = Map.of("timeout-ms", 100);  // 100ms max
results = store.searchWithIds(query, 10, options);

// Early stopping with patience
options = Map.of(
    "patience", 50,                    // Stop after 50 candidates with no improvement
    "patience-saturation", 0.95        // Stop when 95% of best similarity reached
);
results = store.searchWithIds(query, 10, options);

// Minimum similarity threshold
options = Map.of("min-similarity", 0.8);  // Only return results with similarity >= 0.8
results = store.searchWithIds(query, 10, options);

// Combine multiple controls
options = Map.of(
    "ef", 100,
    "timeout-ms", 100,
    "min-similarity", 0.7
);
results = store.searchWithIds(query, 10, options);
```

**Available options:**

| Option | Type | Description |
|--------|------|-------------|
| `ef` | Integer | Beam width (higher = better recall, slower) |
| `timeout-ms` | Integer | Maximum search time in milliseconds |
| `patience` | Integer | Early stopping: candidates to check with no improvement |
| `patience-saturation` | Double | Early stopping: similarity threshold (0.0-1.0) |
| `min-similarity` | Double | Minimum similarity to include in results (0.0-1.0) |

### Search with Metadata

Include metadata in search results:

```java
List<SearchResult> results = store.searchWithMetadata(queryVector, 10);

for (SearchResult r : results) {
    Map<String, Object> metadata = r.getMetadata();
    System.out.println("Title: " + metadata.get("title"));
    System.out.println("Distance: " + r.getDistance());
}
```

### Filtered Search

Search only specific IDs (multi-tenant, per-user, etc.):

```java
// Search only specific documents
Set<Object> allowedIds = Set.of("doc-1", "doc-3", "doc-5");
List<SearchResult> results = store.searchFiltered(queryVector, 10, allowedIds);

// With options
Map<String, Object> options = Map.of("ef", 100);
results = store.searchFiltered(queryVector, 10, allowedIds, options);
```

---

## Versioning & Branching

### Snapshot Semantics

Every operation creates a new snapshot:

```java
ProximumVectorStore v1 = store.add(vecA, "a");
ProximumVectorStore v2 = v1.add(vecB, "b");
ProximumVectorStore v3 = v2.add(vecC, "c");

// All versions are independent and queryable
System.out.println(v1.count());  // => 1
System.out.println(v2.count());  // => 2
System.out.println(v3.count());  // => 3

v1.search(query, 5);  // Search snapshot v1
v3.search(query, 5);  // Search snapshot v3
```

**Use case: Time travel**

```java
// Save reference before modification
ProximumVectorStore beforeUpdate = store;

// Make changes
store = store.add(newVec, "new-doc");
store = store.delete("old-doc");

// Compare search results
List<SearchResult> oldResults = beforeUpdate.search(query, 10);
List<SearchResult> newResults = store.search(query, 10);
```

### Commits and Sync

Create commits to persist state:

```java
// Add vectors (in-memory)
store = store.add(vec1, "doc-1");
store = store.add(vec2, "doc-2");

// Create commit (persist to disk)
store.sync();

// Get commit ID
UUID commitId = store.getCommitId();
System.out.println("Commit: " + commitId);
```

### Branching

Create lightweight branches for experiments:

```java
// Main branch
store = store.add(vec1, "doc-1");
store = store.add(vec2, "doc-2");
store.sync();

// Create experiment branch
ProximumVectorStore experiment = store.branch("experiment");

// Modify experiment
experiment = experiment.add(vec3, "exp-doc-1");
experiment = experiment.add(vec4, "exp-doc-2");

// Main is unchanged
System.out.println(store.count());      // => 2
System.out.println(experiment.count()); // => 4

// Search both independently
store.search(query, 5);      // Production results
experiment.search(query, 5); // Experimental results
```

**List branches:**

```java
Set<String> branches = store.getBranches();
System.out.println("Available branches: " + branches);
```

**Get current branch:**

```java
String currentBranch = store.getCurrentBranch();
System.out.println("On branch: " + currentBranch);
```

**Delete a branch:**

```java
store.deleteBranch("experiment");
```

### Time Travel

Query historical commits:

```java
// Store some data
store = store.add(vec1, "doc-1");
store.sync();
UUID snapshot1 = store.getCommitId();

store = store.add(vec2, "doc-2");
store.sync();
UUID snapshot2 = store.getCommitId();

// Later: Load historical state
Map<String, Object> storeConfig = Map.of(
    "backend", ":file",
    "path", "/var/data/vectors",
    "id", storageId  // Get from original config
);

ProximumVectorStore historical = ProximumVectorStore.connectCommit(
    storeConfig, snapshot1);

// Search sees only doc-1!
List<SearchResult> results = historical.search(query, 10);
```

### History and Commits

**View commit history:**

```java
List<Map<String, Object>> history = store.getHistory();

for (Map<String, Object> commit : history) {
    System.out.println("Commit: " + commit.get("proximum/commit-id"));
    System.out.println("Date: " + commit.get("proximum/created-at"));
    System.out.println("Branch: " + commit.get("proximum/branch"));
    System.out.println("Vector count: " + commit.get("proximum/vector-count"));
}
```

---

## Metadata

Attach arbitrary data to vectors for filtering, display, and context.

### Adding Metadata

```java
// Add vector first
store = store.add(embedding, "doc-1");

// Then add metadata
Map<String, Object> metadata = Map.of(
    "title", "Introduction to AI",
    "author", "Alice",
    "category", "ai",
    "tags", List.of("machine-learning", "tutorial"),
    "timestamp", System.currentTimeMillis()
);
store = store.withMetadata("doc-1", metadata);
```

### Retrieving Metadata

```java
Map<String, Object> metadata = store.getMetadata("doc-1");
if (metadata != null) {
    String title = (String) metadata.get("title");
    String author = (String) metadata.get("author");
    System.out.println(title + " by " + author);
}
```

### Updating Metadata

```java
Map<String, Object> updatedMetadata = Map.of(
    "title", "Updated Title",
    "author", "Alice",
    "updated", true
);
store = store.withMetadata("doc-1", updatedMetadata);
```

### Search with Metadata

Include metadata in search results:

```java
List<SearchResult> results = store.searchWithMetadata(query, 10);

for (SearchResult r : results) {
    Map<String, Object> metadata = r.getMetadata();
    String title = (String) metadata.get("title");
    System.out.printf("%s - Distance: %.4f%n", title, r.getDistance());
}
```

### Filtered Search by Metadata

Build ID sets from metadata predicates:

```java
// Get all documents in a specific category
Set<Object> aiDocIds = new HashSet<>();
// Note: You need to iterate and filter (or maintain external index)
// For large-scale filtering, consider external index like Datahike

// Then search only those IDs
List<SearchResult> results = store.searchFiltered(query, 10, aiDocIds);
```

**Note**: For production-scale metadata filtering, maintain external indices (databases) that map metadata → IDs.

---

## Advanced Features

### Crypto-Hash (Auditability)

Enable cryptographic hashing for compliance and verification:

```java
// Create index with crypto-hash enabled
ProximumVectorStore store = ProximumVectorStore.builder()
    .dimensions(384)
    .storagePath("/var/data/vectors")
    .cryptoHash(true)  // Enable SHA-512
    .build();

store = store.add(vec1, "doc-1");
store.sync();

// Get commit hash (chains like git)
UUID hash1 = store.getCommitHash();
System.out.println("Commit hash: " + hash1);

store = store.add(vec2, "doc-2");
store.sync();

UUID hash2 = store.getCommitHash();
System.out.println("New hash: " + hash2);
System.out.println("Hashes differ: " + !hash1.equals(hash2));

// Verify integrity from cold storage
Map<String, Object> storeConfig = Map.of(
    "backend", ":file",
    "path", "/var/data/vectors",
    "id", storageId
);
Map<String, Object> verification = ProximumVectorStore.verifyFromCold(storeConfig);

System.out.println("Valid: " + verification.get("valid?"));
System.out.println("Vectors verified: " + verification.get("vectors-verified"));
```

**Use cases:**
- **Compliance**: HIPAA, GDPR audit trails
- **Backup verification**: Detect corruption
- **Supply chain**: Verify data integrity
- **Reproducibility**: Exact hash ensures same state

### Compaction

Reclaim space from deleted vectors:

```java
// Check if compaction needed
if (store.isNeedsCompaction()) {
    Map<String, Object> metrics = store.getMetrics();
    double deletionRatio = (Double) metrics.get("deletion-ratio");
    System.out.println("Deletion ratio: " + deletionRatio);

    // Compact (offline operation)
    Map<String, Object> target = Map.of(
        "store-config", Map.of(
            "backend", ":file",
            "path", "/var/data/vectors-new",
            "id", UUID.randomUUID()
        ),
        "mmap-dir", "/var/data/mmap-new"
    );

    ProximumVectorStore compacted = store.compact(target);

    // Result: smaller disk usage, faster searches
    metrics = compacted.getMetrics();
    System.out.println("New deletion ratio: " + metrics.get("deletion-ratio"));
}
```

**When to compact:**
- Deletion ratio > 0.3 (30% deleted)
- Query performance degrading
- Disk space running low
- During maintenance window

### Garbage Collection

Remove unreachable commits and branches:

```java
// GC unreferenced data
Set<Object> deletedKeys = store.gc();
System.out.println("Freed " + deletedKeys.size() + " keys");
```

**What GC does:**
- Removes commits with no branch pointing to them
- Removes data from deleted branches
- Reclaims storage space

**When to GC:**
- After deleting branches
- Periodically (weekly/monthly)
- When storage is running low

### Index Metrics

Get comprehensive health metrics:

```java
Map<String, Object> metrics = store.getMetrics();

// Vector counts
System.out.println("Total vectors: " + metrics.get("vector-count"));
System.out.println("Live vectors: " + metrics.get("live-count"));
System.out.println("Deleted vectors: " + metrics.get("deleted-count"));

// Health
System.out.println("Deletion ratio: " + metrics.get("deletion-ratio"));
System.out.println("Needs compaction: " + metrics.get("needs-compaction?"));
System.out.println("Utilization: " + metrics.get("utilization"));

// Graph statistics
System.out.println("Edge count: " + metrics.get("edge-count"));
System.out.println("Avg edges per node: " + metrics.get("avg-edges-per-node"));

// Current state
System.out.println("Branch: " + metrics.get("branch"));
System.out.println("Commit ID: " + metrics.get("commit-id"));
```

---

## Spring AI Integration

Proximum provides a native Spring AI `VectorStore` adapter:

```java
import org.replikativ.proximum.spring.ProximumVectorStore;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class VectorStoreConfig {

    @Bean
    public VectorStore vectorStore(EmbeddingModel embeddingModel) {
        return new ProximumVectorStore.Builder()
            .dimensions(1536)
            .storagePath("/var/data/vectors")
            .embeddingModel(embeddingModel)
            .build();
    }
}
```

**Usage:**

```java
@Autowired
private VectorStore vectorStore;

public void addDocuments(List<Document> documents) {
    vectorStore.add(documents);
}

public List<Document> search(String query, int k) {
    return vectorStore.similaritySearch(
        SearchRequest.query(query).withTopK(k));
}
```

**Features:**
- Automatic embedding with provided `EmbeddingModel`
- Spring Boot auto-configuration
- Document metadata support
- Filtered search capabilities

---

## LangChain4j Integration

Proximum provides a native LangChain4j `EmbeddingStore` adapter:

```java
import org.replikativ.proximum.langchain4j.ProximumEmbeddingStore;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.EmbeddingStore;

EmbeddingStore<TextSegment> embeddingStore = ProximumEmbeddingStore.builder()
    .dimensions(1536)
    .storagePath("/var/data/embeddings")
    .build();
```

**Usage:**

```java
// Add embeddings
Embedding embedding = embeddingModel.embed("Hello world").content();
String id = embeddingStore.add(embedding);

// Add with metadata
TextSegment segment = TextSegment.from("Hello world", Metadata.from("category", "greeting"));
embeddingStore.add(embedding, segment);

// Search
List<EmbeddingMatch<TextSegment>> matches =
    embeddingStore.findRelevant(queryEmbedding, 10);

for (EmbeddingMatch<TextSegment> match : matches) {
    TextSegment segment = match.embedded();
    System.out.println(segment.text() + " - Score: " + match.score());
}
```

---

## Performance Tips

### 1. Batch Operations

Use batch insert for better performance:

```java
// Slow: Individual adds
for (int i = 0; i < 1000; i++) {
    store = store.add(vectors.get(i), ids.get(i));
}

// Fast: Batch insert
store = store.addBatch(vectors, ids);
```

### 2. Tune HNSW Parameters

| Parameter | Higher = | Lower = | Sweet Spot |
|-----------|----------|---------|------------|
| **M** | Better recall, more memory | Faster build | 16-32 |
| **efConstruction** | Better graph quality | Faster inserts | 100-400 |
| **efSearch** | Better recall | Faster search | 50-200 |

```java
// For recall-critical applications
ProximumVectorStore store = ProximumVectorStore.builder()
    .dimensions(1536)
    .m(32)
    .efConstruction(400)
    .efSearch(200)
    .build();

// For speed-critical applications
store = ProximumVectorStore.builder()
    .dimensions(1536)
    .m(16)
    .efConstruction(100)
    .efSearch(50)
    .build();
```

### 3. Distance Metrics

Choose the right metric for your embeddings:

```java
// For normalized embeddings (unit vectors)
.distance(DistanceMetric.COSINE)

// For unnormalized embeddings
.distance(DistanceMetric.EUCLIDEAN)

// For maximum inner product search
.distance(DistanceMetric.INNER_PRODUCT)
```

### 4. Capacity Planning

Set capacity to expected size:

```java
// If you expect 1M vectors
.capacity(1_000_000)

// Default is 10M (allocates memory even if not filled)
```

### 5. Sync Frequency

Balance durability vs performance:

```java
// High-throughput ingestion: Batch then sync
store = store.addBatch(batch1Vectors, batch1Ids);
store = store.addBatch(batch2Vectors, batch2Ids);
store = store.addBatch(batch3Vectors, batch3Ids);
store.sync();  // Sync once after all batches

// Critical data: Sync immediately
store = store.add(criticalVector, "critical-doc");
store.sync();
```

### 6. Resource Management

Always use try-with-resources:

```java
try (ProximumVectorStore store = ProximumVectorStore.builder()...build()) {
    // Use store
} // Automatically closes and releases resources
```

---

## Troubleshooting

### Issue: "Dimension mismatch"

**Problem**: Vectors don't match index dimensions.

**Solution**: Ensure all vectors have correct dimensionality:

```java
ProximumVectorStore store = ProximumVectorStore.builder()
    .dimensions(384)
    .build();

// Wrong
float[] vector = new float[512];  // Error! 512 ≠ 384
store = store.add(vector, "doc-1");

// Right
float[] vector = new float[384];  // OK
store = store.add(vector, "doc-1");
```

### Issue: "ClassNotFoundException" or "IllegalCallerException"

**Problem**: Missing JVM options for Vector API / Foreign Memory.

**Symptoms**:
- `ClassNotFoundException: jdk.incubator.vector.VectorSpecies`
- `IllegalCallerException: ... does not have native access enabled`
- Application exits with error (not a crash)

**Solution**: Add required JVM options:

```
--add-modules=jdk.incubator.vector
--enable-native-access=ALL-UNNAMED
```

**Maven:**
```xml
<plugin>
  <artifactId>maven-surefire-plugin</artifactId>
  <configuration>
    <argLine>--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED</argLine>
  </configuration>
</plugin>
```

**Gradle:**
```gradle
test {
    jvmArgs '--add-modules=jdk.incubator.vector', '--enable-native-access=ALL-UNNAMED'
}
```

### Issue: "Store already exists"

**Problem**: Trying to create index when storage path already exists.

**Solution**: Use `connect()` to reconnect:

```java
// Delete existing
new File("/var/data/vectors").delete();

// Or reconnect
Map<String, Object> storeConfig = Map.of(
    "backend", ":file",
    "path", "/var/data/vectors",
    "id", existingStorageId
);
ProximumVectorStore store = ProximumVectorStore.connect(storeConfig);
```

### Issue: "Out of capacity"

**Problem**: Exceeded `:capacity` setting.

**Solution**: Create new index with larger capacity, migrate data:

```java
ProximumVectorStore newStore = ProximumVectorStore.builder()
    .dimensions(oldStore.getDimensions())
    .capacity(2_000_000)  // 2x capacity
    .storagePath("/var/data/vectors-new")
    .build();

// Migrate data (pseudo-code - need to iterate old store)
// for each id: newStore = newStore.add(oldStore.getVector(id), id);
```

### Issue: "Slow searches"

**Problem**: Poor recall or high deletion ratio.

**Solutions:**

1. **Increase ef for specific searches**:
   ```java
   Map<String, Object> options = Map.of("ef", 200);
   store.searchWithIds(query, 10, options);
   ```

2. **Compact if deletion ratio high**:
   ```java
   if (store.isNeedsCompaction()) {
       store = store.compact(targetConfig);
   }
   ```

3. **Tune M and efConstruction** when creating index

### Issue: "High memory usage"

**Problem**: Large indices consuming too much RAM.

**Solutions:**

1. **Reduce cache-size**:
   ```java
   .cacheSize(1000)  // Default is 10000
   ```

2. **Reduce M**:
   ```java
   .m(8)  // Default is 16
   ```

3. **Use memory-mapped storage** (automatic for file backend)

---

## Examples

Browse working examples in [`examples/java/`](../examples/java/):

- **QuickStart.java**: Basic usage with builder pattern
- **AuditableIndex.java**: Crypto-hash and verification
- Spring Boot RAG application (coming soon)

---

**Questions?** Open an issue on [GitHub](https://github.com/replikativ/proximum/issues).
