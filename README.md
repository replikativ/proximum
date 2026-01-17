# Proximum

[![Clojars Project](https://img.shields.io/clojars/v/org.replikativ/proximum.svg)](https://clojars.org/org.replikativ/proximum)
[![Slack](https://img.shields.io/badge/slack-join_chat-brightgreen.svg)](https://clojurians.slack.com/archives/CB7GJAN0L)
[![GitHub last commit](https://img.shields.io/github/last-commit/replikativ/proximum/main.svg)](https://github.com/replikativ/proximum)

> ⚠️ **Early Beta**: Proximum is under active development. APIs may change before 1.0 release. Feedback welcome!

> 📋 **Help shape Proximum!** We'd love your input. Please fill out our [2-minute feedback survey](https://docs.google.com/forms/d/e/1FAIpQLSeUQuw5SPyIx661e1pwZiX0100bP-DPpF2Zfpptg1h6k14OTA/viewform).

A high-performance, embeddable vector database for Clojure and Java with **Git-like versioning** and **zero-cost branching**.

## Why Proximum?

Unlike traditional vector databases, Proximum brings **persistent data structure semantics** to vector search:

- ✨ **Time Travel**: Query any historical snapshot
- 🌿 **Zero-Cost Branching**: Fork indices for experiments without copying data
- 🔒 **Immutability**: All operations return new versions, enabling safe concurrency
- 💾 **True Persistence**: Durable storage with structural sharing
- 🚀 **High Performance**: SIMD-accelerated search with competitive recall
- 📦 **Pure JVM**: No native dependencies, works everywhere

Perfect for **RAG applications**, **semantic search**, and **ML experimentation** where you need to track versions, A/B test embeddings, or maintain reproducible search results.

---

## Quick Start

### Clojure

```clojure
(require '[proximum.core :as prox])

;; Create identifier of the underlying storage with (random-uuid)
(def store-id #uuid "465df026-fcd3-4cb3-be44-29a929776250") 

;; Create an index - feels like Clojure!
(def idx (prox/create-index {:type :hnsw
                              :dim 384
                              :store-config {:backend :memory
                                             :id store-id}
                              :capacity 10000}))

;; Use collection protocols
(def idx2 (assoc idx "doc-1" (float-array (repeatedly 384 rand))))
(def idx3 (assoc idx2 "doc-2" (float-array (repeatedly 384 rand))))

;; Search for nearest neighbors
(def results (prox/search idx3 (float-array (repeatedly 384 rand)) 5))
; => ({:id "doc-1", :distance 0.234} {:id "doc-2", :distance 0.456} ...)

;; Git-like branching
(prox/sync! idx3)
(def experiment (prox/branch! idx3 "experiment"))
```

📖 [Full Clojure Guide](docs/CLOJURE_GUIDE.md)

### Java

```java
import org.replikativ.proximum.*;

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
    // => [SearchResult{id=doc-1, distance=0.234}, ...]

    // Git-like versioning
    store.sync();  // Create commit
    UUID snapshot1 = store.getCommitId();

    store = store.add(embedding3, "doc-3");
    store.sync();

    // Time travel: Query historical state
    ProximumVectorStore historical = ProximumVectorStore.connectCommit(
        Map.of("backend", ":file", "path", "/tmp/vectors"), snapshot1);
    historical.search(queryVector, 5);  // Only sees doc-1, doc-2!

    // Branch for experiments
    ProximumVectorStore experiment = store.branch("experiment");
}
```

📖 [Full Java Guide](docs/JAVA_GUIDE.md)

---

## Installation

[![Clojars Project](https://img.shields.io/clojars/v/org.replikativ/proximum.svg)](https://clojars.org/org.replikativ/proximum)

### Clojure (deps.edn)

```clojure
{:deps {org.replikativ/proximum {:mvn/version "LATEST"}}}
```

### Leiningen (project.clj)

```clojure
[org.replikativ/proximum "LATEST"]
```

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

---

## Key Features

### 🔄 Versioning & Time Travel

Every `sync()` creates a commit. Query any historical state:

```java
index.sync();  // Snapshot 1
// ... make changes ...
index.sync();  // Snapshot 2

// Time travel to earlier state
ProximumVectorStore historical = index.asOf(commitId);
```

**Use Cases:** Audit trails, debugging, A/B testing, reproducible results

### 🌿 Zero-Cost Branching

Fork an index for experiments without copying data:

```java
index.sync();
ProximumVectorStore experiment = index.branch("new-model");

// Test different embeddings
experiment.add(newEmbedding, "doc-1");

// Merge or discard - original unchanged
```

**Use Cases:** A/B testing, staging, parallel experiments

### 🔍 Advanced Features

- **Filtered Search**: Multi-tenant search with ID filtering
- **Metadata**: Attach arbitrary metadata to vectors
- **Compaction**: Reclaim space from deleted vectors
- **Garbage Collection**: Clean up unreachable commits
- **[Crypto-Hash](docs/CRYPTO_HASH_GUIDE.md)**: Tamper-proof audit trail with SHA-512

---

## Integrations

### Spring AI

```java
import org.replikativ.proximum.spring.ProximumVectorStore;

@Bean
public VectorStore vectorStore() {
    return ProximumVectorStore.builder()
        .dimensions(1536)
        .storagePath("/data/vectors")
        .build();
}
```

📖 [Spring AI Integration Guide](docs/SPRING_AI_GUIDE.md) | [Spring Boot RAG Example](examples/spring-boot-rag/)

### LangChain4j

```java
import org.replikativ.proximum.langchain4j.ProximumEmbeddingStore;

EmbeddingStore<TextSegment> store = ProximumEmbeddingStore.builder()
    .dimensions(1536)
    .storagePath("/data/embeddings")
    .build();
```

📖 [LangChain4j Integration Guide](docs/LANGCHAIN4J_GUIDE.md)

---

## Performance

**SIFT-1M (1M vectors, 128-dim, Intel Core Ultra 7):**

| Implementation | Search QPS | Insert (vec/s) | p50 Latency | Recall@10 |
|----------------|------------|----------------|-------------|-----------|
| hnswlib (C++) | 7,849 | 18,205 | 131 µs | 98.32% |
| **Proximum** | **3,750** (48%) | 9,621 | **262 µs** | **98.66%** |
| lucene-hnsw | 3,095 (39%) | 2,347 | 333 µs | 98.53% |
| jvector | 1,844 (23%) | 6,095 | 557 µs | 95.95% |
| hnswlib-java | 1,004 (13%) | 4,329 | 1,041 µs | 98.30% |

**Proximum metrics:**
- Storage: 762.8 MB
- Heap usage: 545.7 MB

**Key features:**
- Pure JVM with SIMD acceleration (Java Vector API)
- No native dependencies, works on all platforms
- Persistent storage with zero-cost branching

---

## Documentation

**API Guides:**
- [Clojure Guide](docs/CLOJURE_GUIDE.md) - Complete Clojure API with collection protocols
- [Java Guide](docs/JAVA_GUIDE.md) - Builder pattern, immutability, and best practices

**Integration Guides:**
- [Spring AI Guide](docs/SPRING_AI_GUIDE.md) - Spring Boot RAG applications
- [LangChain4j Guide](docs/LANGCHAIN4J_GUIDE.md) - LangChain4j embedding store integration

**Advanced Topics:**
- [Cryptographic Auditability](docs/CRYPTO_HASH_GUIDE.md) - Tamper-proof commit hashing and verification
- [Persistence Design](docs/PERSISTENCE.md) - Internal persistence mechanisms (PES, VectorStorage, PSS)

**Examples:**
- [Spring Boot RAG Example](examples/spring-boot-rag/) - Full-featured RAG application with versioning

---

## Examples

Browse working examples in [`examples/`](examples/):

- **Clojure**: Semantic search, RAG, collection protocols
- **Java**: Quick start, auditable index, metadata usage

**Demo Projects:**
- **[Einbetten](https://github.com/replikativ/einbetten)**: Wikipedia semantic search with Datahike + FastEmbed (2,000 articles, ~8,000 chunks)

---

## Requirements

- **Java**: 22+ (Foreign Memory API finalized in Java 22)
- **OS**: Linux, macOS, Windows
- **CPU**: AVX2 recommended, AVX-512 for best performance

**JVM Options Required:**
```bash
--add-modules=jdk.incubator.vector
--enable-native-access=ALL-UNNAMED
```

---

## License

EPL-2.0 (Eclipse Public License 2.0) - see [LICENSE](LICENSE)

---

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for:
- Code of conduct
- Development workflow
- Testing requirements
- Licensing (DCO/EPL-2.0)

---

## Support

- **Issues**: [GitHub Issues](https://github.com/replikativ/proximum/issues)
- **Discussions**: [GitHub Discussions](https://github.com/replikativ/proximum/discussions)
- **Commercial Support**: contact@datahike.io

---

Built with ❤️ by the replikativ team
