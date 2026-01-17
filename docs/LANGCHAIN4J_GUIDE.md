# LangChain4j Integration Guide

Complete guide for using Proximum with LangChain4j's EmbeddingStore API.

> **LangChain4j Version**: Tested with 0.35.0+

## Table of Contents

1. [Quick Start](#quick-start)
2. [Configuration](#configuration)
3. [Basic Operations](#basic-operations)
4. [RAG with AI Services](#rag-with-ai-services)
5. [Metadata Filtering](#metadata-filtering)
6. [Content Retrieval](#content-retrieval)
7. [Conversational Memory](#conversational-memory)
8. [Version Control Features](#version-control-features)
9. [Advanced Use Cases](#advanced-use-cases)
10. [Production Deployment](#production-deployment)
11. [Troubleshooting](#troubleshooting)

---

## Quick Start

### Dependencies (Maven)

```xml
<dependencies>
    <!-- LangChain4j Core -->
    <dependency>
        <groupId>dev.langchain4j</groupId>
        <artifactId>langchain4j</artifactId>
        <version>0.35.0</version>
    </dependency>

    <!-- LangChain4j OpenAI Integration -->
    <dependency>
        <groupId>dev.langchain4j</groupId>
        <artifactId>langchain4j-open-ai</artifactId>
        <version>0.35.0</version>
    </dependency>

    <!-- Proximum Vector Store -->
    <dependency>
        <groupId>org.replikativ</groupId>
        <artifactId>proximum</artifactId>
        <version>LATEST</version>
    </dependency>
</dependencies>

<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-surefire-plugin</artifactId>
            <configuration>
                <argLine>
                    --add-modules=jdk.incubator.vector
                    --enable-native-access=ALL-UNNAMED
                </argLine>
            </configuration>
        </plugin>
    </plugins>
</build>
```

### Dependencies (Gradle)

```gradle
dependencies {
    implementation 'dev.langchain4j:langchain4j:0.35.0'
    implementation 'dev.langchain4j:langchain4j-open-ai:0.35.0'
    implementation 'org.replikativ:proximum:LATEST'
}

tasks.withType(JavaExec) {
    jvmArgs = [
        '--add-modules=jdk.incubator.vector',
        '--enable-native-access=ALL-UNNAMED'
    ]
}
```

### Basic Usage

```java
import org.replikativ.proximum.langchain4j.ProximumEmbeddingStore;
import org.replikativ.proximum.DistanceMetric;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;

// Create embedding model
EmbeddingModel embeddingModel = OpenAiEmbeddingModel.builder()
        .apiKey(System.getenv("OPENAI_API_KEY"))
        .modelName("text-embedding-ada-002")
        .build();

// Create embedding store
EmbeddingStore<TextSegment> store = ProximumEmbeddingStore.builder()
        .storagePath("/var/data/embeddings")
        .dimensions(1536)  // OpenAI ada-002
        .distance(DistanceMetric.COSINE)
        .build();

// Add a document
TextSegment segment = TextSegment.from("Proximum is a vector database with version control");
Embedding embedding = embeddingModel.embed(segment).content();
String id = store.add(embedding, segment);

// Search
Embedding queryEmbedding = embeddingModel.embed("What is Proximum?").content();
EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
        .queryEmbedding(queryEmbedding)
        .maxResults(5)
        .build();
EmbeddingSearchResult<TextSegment> results = store.search(request);
```

---

## Configuration

### Builder Parameters

```java
EmbeddingStore<TextSegment> store = ProximumEmbeddingStore.builder()
        .storagePath("/var/data/embeddings")  // Required: persistent storage path
        .dimensions(1536)                      // Required: embedding dimensions
        .distance(DistanceMetric.COSINE)       // Optional: COSINE, EUCLIDEAN, DOTPRODUCT
        .m(16)                                 // Optional: HNSW connectivity (default: 16)
        .efConstruction(200)                   // Optional: build quality (default: 200)
        .efSearch(50)                          // Optional: search quality (default: 50)
        .capacity(100000)                      // Optional: max vectors (default: 10M)
        .build();
```

### Distance Metrics

```java
// Best for normalized embeddings (most common)
DistanceMetric.COSINE

// For non-normalized vectors
DistanceMetric.EUCLIDEAN

// For dot product similarity
DistanceMetric.DOTPRODUCT
```

### Connecting to Existing Store

```java
// Connect to existing persistent store
EmbeddingStore<TextSegment> store = ProximumEmbeddingStore.connect(
        "/var/data/embeddings",  // storage path
        1536                      // dimensions
);
```

---

## Basic Operations

### Adding Embeddings

#### Single Embedding with Auto-ID

```java
Embedding embedding = embeddingModel.embed("Vector databases enable semantic search").content();
TextSegment segment = TextSegment.from("Vector databases enable semantic search");
String id = store.add(embedding, segment);  // Returns auto-generated UUID
```

#### Single Embedding with Custom ID

```java
Embedding embedding = embeddingModel.embed("HNSW graphs provide fast search").content();
store.add("custom-doc-id", embedding);
```

#### With Metadata

```java
import dev.langchain4j.data.document.Metadata;

TextSegment segment = TextSegment.from(
        "Proximum provides git-like version control",
        Metadata.from("category", "features").put("year", 2024)
);
Embedding embedding = embeddingModel.embed(segment).content();
String id = store.add(embedding, segment);
```

#### Batch Operations

```java
List<TextSegment> segments = List.of(
        TextSegment.from("Document 1"),
        TextSegment.from("Document 2"),
        TextSegment.from("Document 3")
);

// Embed all documents
List<Embedding> embeddings = segments.stream()
        .map(segment -> embeddingModel.embed(segment).content())
        .collect(Collectors.toList());

// Add all at once
List<String> ids = store.addAll(embeddings, segments);
```

### Searching

#### Basic Search

```java
String query = "How does semantic search work?";
Embedding queryEmbedding = embeddingModel.embed(query).content();

EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
        .queryEmbedding(queryEmbedding)
        .maxResults(10)
        .build();

EmbeddingSearchResult<TextSegment> results = store.search(request);

for (EmbeddingMatch<TextSegment> match : results.matches()) {
    System.out.printf("Score: %.3f, Text: %s%n",
            match.score(),
            match.embedded().text());
}
```

#### Search with Similarity Threshold

```java
EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
        .queryEmbedding(queryEmbedding)
        .maxResults(10)
        .minScore(0.7)  // Only return results with score >= 0.7
        .build();

EmbeddingSearchResult<TextSegment> results = store.search(request);
```

### Deleting Embeddings

```java
// Delete single embedding
store.remove("doc-id");

// Delete multiple embeddings
List<String> idsToDelete = List.of("doc-1", "doc-2", "doc-3");
store.removeAll(idsToDelete);
```

### Counting Embeddings

```java
ProximumEmbeddingStore proximumStore = (ProximumEmbeddingStore) store;
long count = proximumStore.count();
System.out.println("Total embeddings: " + count);
```

---

## RAG with AI Services

LangChain4j's **AI Services** provide high-level abstractions for building RAG applications.

### Simple RAG Assistant

```java
import dev.langchain4j.service.AiServices;
import dev.langchain4j.rag.content.retriever.EmbeddingStoreContentRetriever;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.openai.OpenAiChatModel;

interface Assistant {
    String chat(String userMessage);
}

// Create chat model
ChatLanguageModel chatModel = OpenAiChatModel.builder()
        .apiKey(System.getenv("OPENAI_API_KEY"))
        .modelName("gpt-4")
        .build();

// Create content retriever
ContentRetriever contentRetriever = EmbeddingStoreContentRetriever.builder()
        .embeddingStore(store)
        .embeddingModel(embeddingModel)
        .maxResults(5)
        .minScore(0.7)
        .build();

// Build AI Service
Assistant assistant = AiServices.builder(Assistant.class)
        .chatLanguageModel(chatModel)
        .contentRetriever(contentRetriever)
        .build();

// Use it!
String answer = assistant.chat("What is Proximum?");
System.out.println(answer);
```

### RAG with Conversation Memory

```java
import dev.langchain4j.memory.chat.MessageWindowChatMemory;

interface Assistant {
    String chat(@UserMessage String userMessage);
}

Assistant assistant = AiServices.builder(Assistant.class)
        .chatLanguageModel(chatModel)
        .contentRetriever(contentRetriever)
        .chatMemory(MessageWindowChatMemory.withMaxMessages(10))
        .build();

// Multi-turn conversation
assistant.chat("What is Proximum?");
assistant.chat("What are its unique features?");  // Context preserved
assistant.chat("How does it compare to Pinecone?");  // Full conversation history
```

### Advanced RAG with System Instructions

```java
interface Assistant {
    @SystemMessage("You are a helpful AI assistant specializing in vector databases. " +
                   "Answer questions based on the provided context. " +
                   "If you don't know the answer, say so.")
    String chat(@UserMessage String userMessage);
}

Assistant assistant = AiServices.builder(Assistant.class)
        .chatLanguageModel(chatModel)
        .contentRetriever(contentRetriever)
        .chatMemory(MessageWindowChatMemory.withMaxMessages(10))
        .build();
```

---

## Metadata Filtering

Proximum's LangChain4j adapter supports full metadata filtering using LangChain4j's Filter API.

### Filter Syntax

```java
import dev.langchain4j.store.embedding.filter.Filter;
import dev.langchain4j.store.embedding.filter.MetadataFilterBuilder;

// Simple equality
Filter techFilter = MetadataFilterBuilder.metadataKey("category").isEqualTo("tech");

// Numeric comparison
Filter recentFilter = MetadataFilterBuilder.metadataKey("year").isGreaterThanOrEqualTo(2024);

// IN operator
Filter categoryFilter = MetadataFilterBuilder.metadataKey("category")
        .isIn("tech", "science", "engineering");

// NOT IN operator
Filter excludeFilter = MetadataFilterBuilder.metadataKey("status")
        .isNotIn("archived", "deleted");

// Combining filters with AND
Filter techRecent = Filter.and(
        MetadataFilterBuilder.metadataKey("category").isEqualTo("tech"),
        MetadataFilterBuilder.metadataKey("year").isGreaterThanOrEqualTo(2024)
);

// Combining filters with OR
Filter multiCategory = Filter.or(
        MetadataFilterBuilder.metadataKey("category").isEqualTo("tech"),
        MetadataFilterBuilder.metadataKey("category").isEqualTo("science")
);

// Complex nested filters
Filter complex = Filter.and(
        MetadataFilterBuilder.metadataKey("category").isIn("tech", "science"),
        Filter.or(
                MetadataFilterBuilder.metadataKey("year").isGreaterThan(2023),
                MetadataFilterBuilder.metadataKey("rating").isGreaterThan(4.5)
        )
);
```

### Using Filters in Search

```java
// Add documents with metadata
TextSegment doc1 = TextSegment.from(
        "Vector databases for AI applications",
        Metadata.from("category", "tech").put("year", 2024).put("rating", 4.8)
);
TextSegment doc2 = TextSegment.from(
        "Machine learning fundamentals",
        Metadata.from("category", "science").put("year", 2023).put("rating", 4.5)
);

store.add(embeddingModel.embed(doc1).content(), doc1);
store.add(embeddingModel.embed(doc2).content(), doc2);

// Search with filter
Filter techFilter = MetadataFilterBuilder.metadataKey("category").isEqualTo("tech");

EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
        .queryEmbedding(queryEmbedding)
        .maxResults(10)
        .filter(techFilter)
        .build();

EmbeddingSearchResult<TextSegment> results = store.search(request);
```

### Multi-Tenancy with Filters

```java
// Add documents with tenant ID
TextSegment tenantADoc = TextSegment.from(
        "Tenant A's confidential data",
        Metadata.from("tenant_id", "tenant-a").put("access_level", "confidential")
);
TextSegment tenantBDoc = TextSegment.from(
        "Tenant B's public data",
        Metadata.from("tenant_id", "tenant-b").put("access_level", "public")
);

store.add(embeddingModel.embed(tenantADoc).content(), tenantADoc);
store.add(embeddingModel.embed(tenantBDoc).content(), tenantBDoc);

// Query only Tenant A's data
Filter tenantFilter = MetadataFilterBuilder.metadataKey("tenant_id").isEqualTo("tenant-a");

EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
        .queryEmbedding(queryEmbedding)
        .maxResults(10)
        .filter(tenantFilter)
        .build();

EmbeddingSearchResult<TextSegment> results = store.search(request);
```

---

## Content Retrieval

### EmbeddingStoreContentRetriever

```java
import dev.langchain4j.rag.content.retriever.EmbeddingStoreContentRetriever;

// Basic retriever
ContentRetriever retriever = EmbeddingStoreContentRetriever.builder()
        .embeddingStore(store)
        .embeddingModel(embeddingModel)
        .maxResults(5)
        .build();

// Retriever with filtering
ContentRetriever filteredRetriever = EmbeddingStoreContentRetriever.builder()
        .embeddingStore(store)
        .embeddingModel(embeddingModel)
        .maxResults(5)
        .minScore(0.7)
        .filter(MetadataFilterBuilder.metadataKey("category").isEqualTo("tech"))
        .build();
```

### Custom Content Retrieval

```java
import dev.langchain4j.rag.content.Content;
import dev.langchain4j.rag.query.Query;

ContentRetriever customRetriever = new ContentRetriever() {
    @Override
    public List<Content> retrieve(Query query) {
        Embedding embedding = embeddingModel.embed(query.text()).content();

        Filter filter = buildDynamicFilter(query.metadata());

        EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
                .queryEmbedding(embedding)
                .maxResults(10)
                .filter(filter)
                .build();

        return store.search(request).matches().stream()
                .map(match -> Content.from(match.embedded().text()))
                .collect(Collectors.toList());
    }

    private Filter buildDynamicFilter(Metadata metadata) {
        // Build filter based on query metadata
        String category = metadata.getString("category");
        if (category != null) {
            return MetadataFilterBuilder.metadataKey("category").isEqualTo(category);
        }
        return null;
    }
};
```

---

## Conversational Memory

### Message Window Memory

```java
import dev.langchain4j.memory.chat.MessageWindowChatMemory;

interface Assistant {
    String chat(@UserMessage String message);
}

Assistant assistant = AiServices.builder(Assistant.class)
        .chatLanguageModel(chatModel)
        .contentRetriever(contentRetriever)
        .chatMemory(MessageWindowChatMemory.withMaxMessages(10))  // Keep last 10 messages
        .build();
```

### Token Window Memory

```java
import dev.langchain4j.memory.chat.TokenWindowChatMemory;

Assistant assistant = AiServices.builder(Assistant.class)
        .chatLanguageModel(chatModel)
        .contentRetriever(contentRetriever)
        .chatMemory(TokenWindowChatMemory.withMaxTokens(1000, OpenAiTokenizer.GPT_4))
        .build();
```

### Persistent Memory

```java
import dev.langchain4j.memory.ChatMemory;
import dev.langchain4j.data.message.ChatMessage;

// Custom persistent memory implementation
class PersistentChatMemory implements ChatMemory {
    private final Map<Object, List<ChatMessage>> messagesByConversationId = new ConcurrentHashMap<>();

    @Override
    public void add(ChatMessage message) {
        // Save to database
    }

    @Override
    public List<ChatMessage> messages() {
        // Load from database
        return List.of();
    }

    @Override
    public void clear() {
        // Clear database
    }
}

Assistant assistant = AiServices.builder(Assistant.class)
        .chatLanguageModel(chatModel)
        .contentRetriever(contentRetriever)
        .chatMemory(new PersistentChatMemory())
        .build();
```

---

## Version Control Features

**Proximum's unique differentiator**: Git-like version control for vector indices.

### Syncing and Commits

```java
ProximumEmbeddingStore proximumStore = (ProximumEmbeddingStore) store;

// Add some documents
store.add(embedding1, segment1);
store.add(embedding2, segment2);

// Sync to create a commit
proximumStore.sync();

// Access underlying store for version control
org.replikativ.proximum.ProximumVectorStore coreStore = proximumStore.getStore();
UUID commitId = coreStore.getCommitId();
System.out.println("Created commit: " + commitId);
```

### Snapshots and Branching

```java
import org.replikativ.proximum.ProximumVectorStore;

// Get the core store
ProximumVectorStore coreStore = ((ProximumEmbeddingStore) store).getStore();

// Create a snapshot
coreStore = coreStore.sync();
UUID snapshot1 = coreStore.getCommitId();

// Add more documents
store.add(embedding3, segment3);
coreStore.sync();

// Create a branch for experiments
ProximumVectorStore experimentBranch = coreStore.branch("experiment");

// Now you have two independent copies:
// - coreStore: production data
// - experimentBranch: experimental changes
```

### Time-Travel Queries

```java
// Create initial snapshot
ProximumEmbeddingStore store1 = ProximumEmbeddingStore.builder()
        .storagePath("/var/data/embeddings")
        .dimensions(1536)
        .build();

store1.add(embedding1, TextSegment.from("Document 1"));
store1.add(embedding2, TextSegment.from("Document 2"));
store1.sync();

ProximumVectorStore coreStore = store1.getStore();
UUID historicalCommitId = coreStore.getCommitId();

// Add more documents
store1.add(embedding3, TextSegment.from("Document 3"));
store1.sync();

// Connect to historical state
ProximumVectorStore historicalCore = ProximumVectorStore.connectCommit(
        Map.of("backend", ":file", "path", "/var/data/embeddings"),
        historicalCommitId
);

ProximumEmbeddingStore historicalStore = new ProximumEmbeddingStore(historicalCore, 1536);

// Search historical state (only sees doc 1 and 2)
EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
        .queryEmbedding(queryEmbedding)
        .maxResults(10)
        .build();

EmbeddingSearchResult<TextSegment> historicalResults = historicalStore.search(request);
// Results will only contain Document 1 and Document 2!
```

### Commit History and Auditability

```java
ProximumVectorStore coreStore = ((ProximumEmbeddingStore) store).getStore();

// Get commit history
List<Map<String, Object>> history = coreStore.getHistory();

for (Map<String, Object> commit : history) {
    System.out.println("Commit ID: " + commit.get("proximum/commit-id"));
    System.out.println("Commit Hash: " + commit.get("proximum/commit-hash"));  // SHA-512
    System.out.println("Created At: " + commit.get("proximum/created-at"));
    System.out.println("Vector Count: " + commit.get("proximum/vector-count"));
}
```

---

## Advanced Use Cases

### Multi-Model Embeddings

```java
// Use different embedding models for different types of content
EmbeddingModel technicalModel = OpenAiEmbeddingModel.builder()
        .apiKey(System.getenv("OPENAI_API_KEY"))
        .modelName("text-embedding-3-large")  // Better for technical content
        .build();

EmbeddingModel generalModel = OpenAiEmbeddingModel.builder()
        .apiKey(System.getenv("OPENAI_API_KEY"))
        .modelName("text-embedding-ada-002")  // Faster, cheaper
        .build();

// Add technical documents with technical model
TextSegment techDoc = TextSegment.from(
        "HNSW algorithm complexity analysis",
        Metadata.from("type", "technical")
);
Embedding techEmbedding = technicalModel.embed(techDoc).content();
store.add(techEmbedding, techDoc);

// Add general documents with general model
TextSegment generalDoc = TextSegment.from(
        "Company news and updates",
        Metadata.from("type", "general")
);
Embedding generalEmbedding = generalModel.embed(generalDoc).content();
store.add(generalEmbedding, generalDoc);

// Filter searches by document type
Filter techFilter = MetadataFilterBuilder.metadataKey("type").isEqualTo("technical");
```

### Hybrid Search (Vector + Keyword)

```java
import dev.langchain4j.rag.content.retriever.ContentRetriever;

class HybridRetriever implements ContentRetriever {
    private final EmbeddingStore<TextSegment> vectorStore;
    private final EmbeddingModel embeddingModel;
    private final KeywordIndex keywordIndex;  // Your keyword search implementation

    @Override
    public List<Content> retrieve(Query query) {
        // Get vector search results
        Embedding embedding = embeddingModel.embed(query.text()).content();
        List<EmbeddingMatch<TextSegment>> vectorResults = vectorStore.search(
                EmbeddingSearchRequest.builder()
                        .queryEmbedding(embedding)
                        .maxResults(10)
                        .build()
        ).matches();

        // Get keyword search results
        List<TextSegment> keywordResults = keywordIndex.search(query.text(), 10);

        // Merge and re-rank results
        return mergeResults(vectorResults, keywordResults);
    }

    private List<Content> mergeResults(
            List<EmbeddingMatch<TextSegment>> vectorResults,
            List<TextSegment> keywordResults
    ) {
        // Implement your ranking strategy (e.g., Reciprocal Rank Fusion)
        return List.of();
    }
}
```

### A/B Testing Retrieval Strategies

```java
class ABTestRetriever implements ContentRetriever {
    private final ContentRetriever strategyA;
    private final ContentRetriever strategyB;
    private final Random random = new Random();

    @Override
    public List<Content> retrieve(Query query) {
        boolean useA = random.nextBoolean();

        if (useA) {
            logMetric("retrieval-strategy", "A");
            return strategyA.retrieve(query);
        } else {
            logMetric("retrieval-strategy", "B");
            return strategyB.retrieve(query);
        }
    }

    private void logMetric(String key, String value) {
        // Log to your analytics platform
    }
}

// Strategy A: High precision
ContentRetriever precisionRetriever = EmbeddingStoreContentRetriever.builder()
        .embeddingStore(store)
        .embeddingModel(embeddingModel)
        .maxResults(3)
        .minScore(0.8)
        .build();

// Strategy B: High recall
ContentRetriever recallRetriever = EmbeddingStoreContentRetriever.builder()
        .embeddingStore(store)
        .embeddingModel(embeddingModel)
        .maxResults(10)
        .minScore(0.6)
        .build();

Assistant assistant = AiServices.builder(Assistant.class)
        .chatLanguageModel(chatModel)
        .contentRetriever(new ABTestRetriever(precisionRetriever, recallRetriever))
        .build();
```

### Document Ingestion Pipeline

```java
import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import dev.langchain4j.data.document.loader.FileSystemDocumentLoader;
import dev.langchain4j.data.document.parser.TextDocumentParser;

class DocumentIngestionPipeline {
    private final EmbeddingStore<TextSegment> store;
    private final EmbeddingModel embeddingModel;
    private final DocumentSplitter splitter;

    public DocumentIngestionPipeline(
            EmbeddingStore<TextSegment> store,
            EmbeddingModel embeddingModel
    ) {
        this.store = store;
        this.embeddingModel = embeddingModel;
        this.splitter = DocumentSplitters.recursive(500, 100);
    }

    public void ingest(Path filePath) {
        // Load document
        Document document = FileSystemDocumentLoader.loadDocument(
                filePath,
                new TextDocumentParser()
        );

        // Split into chunks
        List<TextSegment> segments = splitter.split(document);

        // Embed and store
        for (TextSegment segment : segments) {
            Embedding embedding = embeddingModel.embed(segment).content();
            store.add(embedding, segment);
        }

        // Sync to create commit
        ((ProximumEmbeddingStore) store).sync();
    }
}

// Usage
DocumentIngestionPipeline pipeline = new DocumentIngestionPipeline(store, embeddingModel);
pipeline.ingest(Paths.get("/docs/manual.txt"));
```

---

## Production Deployment

### JVM Options

Proximum requires Java 21+ with Vector API support:

```bash
java --add-modules=jdk.incubator.vector \
     --enable-native-access=ALL-UNNAMED \
     -jar app.jar
```

### Docker

```dockerfile
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

COPY target/app.jar app.jar

EXPOSE 8080

CMD ["java", \
     "--add-modules=jdk.incubator.vector", \
     "--enable-native-access=ALL-UNNAMED", \
     "-Xmx4g", \
     "-jar", "app.jar"]
```

### Connection Pooling

```java
// Reuse embedding store instance across requests
public class EmbeddingStoreManager {
    private static final EmbeddingStore<TextSegment> INSTANCE =
            ProximumEmbeddingStore.builder()
                    .storagePath(System.getenv("STORAGE_PATH"))
                    .dimensions(1536)
                    .build();

    public static EmbeddingStore<TextSegment> getInstance() {
        return INSTANCE;
    }
}
```

### Periodic Syncing

```java
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// Auto-sync every 5 minutes
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(() -> {
    try {
        ((ProximumEmbeddingStore) store).sync();
        System.out.println("Store synced at " + Instant.now());
    } catch (Exception e) {
        System.err.println("Sync failed: " + e.getMessage());
    }
}, 5, 5, TimeUnit.MINUTES);
```

### Resource Cleanup

```java
// Implement AutoCloseable for proper cleanup
class ManagedEmbeddingStore implements AutoCloseable {
    private final ProximumEmbeddingStore store;

    public ManagedEmbeddingStore(ProximumEmbeddingStore store) {
        this.store = store;
    }

    public EmbeddingStore<TextSegment> getStore() {
        return store;
    }

    @Override
    public void close() {
        store.sync();
        store.close();
    }
}

// Usage with try-with-resources
try (ManagedEmbeddingStore managed = new ManagedEmbeddingStore(store)) {
    EmbeddingStore<TextSegment> embeddingStore = managed.getStore();
    // Use store
}  // Automatically synced and closed
```

---

## Troubleshooting

### ClassNotFoundException: jdk.incubator.vector.VectorSpecies

**Problem**: Missing Vector API module.

**Solution**: Add JVM options:
```bash
--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED
```

### java.lang.UnsatisfiedLinkError

**Problem**: Native access not enabled.

**Solution**: Add `--enable-native-access=ALL-UNNAMED` to JVM options.

### Slow Searches

**Problem**: Search latency too high.

**Solutions**:
1. Increase `efSearch` parameter when building store:
```java
ProximumEmbeddingStore.builder()
        .efSearch(100)  // Higher = better recall, slower
        .build();
```

2. Check HNSW parameters (m, efConstruction)
3. Use filters to reduce search space

### High Memory Usage

**Problem**: Large indices consuming too much memory.

**Solutions**:
1. Proximum uses SoftReference caching - JVM will free memory under pressure
2. Adjust JVM heap: `-Xmx4g`
3. Monitor metrics and compact if deletion ratio is high

### Embedding Dimension Mismatch

**Problem**: `IllegalArgumentException: Vector dimension mismatch`

**Solution**: Ensure store dimensions match your embedding model:

```java
// OpenAI ada-002: 1536 dimensions
ProximumEmbeddingStore.builder()
        .dimensions(1536)
        .build();

// OpenAI text-embedding-3-small: 512 or 1536 dimensions
ProximumEmbeddingStore.builder()
        .dimensions(512)
        .build();
```

**Common model dimensions**:
- OpenAI ada-002: 1536
- OpenAI text-embedding-3-small: 512 or 1536
- OpenAI text-embedding-3-large: 3072 (can be reduced)
- Sentence Transformers (all-MiniLM-L6-v2): 384
- Cohere embed-english-v3.0: 1024

### Store Not Persisting Changes

**Problem**: Data lost after restart.

**Solution**: Call `sync()` periodically:
```java
((ProximumEmbeddingStore) store).sync();
```

### Filter Not Working

**Problem**: Filtered search returns unexpected results.

**Solutions**:
1. Verify metadata is stored correctly:
```java
TextSegment segment = TextSegment.from("text", Metadata.from("key", "value"));
```

2. Use correct filter syntax:
```java
Filter filter = MetadataFilterBuilder.metadataKey("category").isEqualTo("tech");
```

3. Check metadata types match (e.g., use `2024` not `"2024"` for integers)

---

## Complete Example Application

```java
import dev.langchain4j.service.AiServices;
import dev.langchain4j.service.UserMessage;
import dev.langchain4j.service.SystemMessage;
import dev.langchain4j.rag.content.retriever.EmbeddingStoreContentRetriever;
import dev.langchain4j.memory.chat.MessageWindowChatMemory;
import org.replikativ.proximum.langchain4j.ProximumEmbeddingStore;

public class RagApplication {

    interface Assistant {
        @SystemMessage("You are a helpful AI assistant. Answer questions based on the provided context.")
        String chat(@UserMessage String message);
    }

    public static void main(String[] args) {
        // Setup
        EmbeddingModel embeddingModel = OpenAiEmbeddingModel.builder()
                .apiKey(System.getenv("OPENAI_API_KEY"))
                .modelName("text-embedding-ada-002")
                .build();

        EmbeddingStore<TextSegment> store = ProximumEmbeddingStore.builder()
                .storagePath("/tmp/embeddings")
                .dimensions(1536)
                .build();

        // Ingest documents
        List<String> documents = List.of(
                "Proximum is a vector database with git-like version control",
                "It supports snapshots, branches, and time-travel queries",
                "Proximum is written in Clojure and Java"
        );

        for (String doc : documents) {
            TextSegment segment = TextSegment.from(doc);
            Embedding embedding = embeddingModel.embed(segment).content();
            store.add(embedding, segment);
        }

        ((ProximumEmbeddingStore) store).sync();

        // Create assistant
        ChatLanguageModel chatModel = OpenAiChatModel.builder()
                .apiKey(System.getenv("OPENAI_API_KEY"))
                .modelName("gpt-4")
                .build();

        ContentRetriever retriever = EmbeddingStoreContentRetriever.builder()
                .embeddingStore(store)
                .embeddingModel(embeddingModel)
                .maxResults(3)
                .build();

        Assistant assistant = AiServices.builder(Assistant.class)
                .chatLanguageModel(chatModel)
                .contentRetriever(retriever)
                .chatMemory(MessageWindowChatMemory.withMaxMessages(10))
                .build();

        // Use assistant
        String answer = assistant.chat("What is Proximum?");
        System.out.println(answer);

        answer = assistant.chat("What are its unique features?");
        System.out.println(answer);

        // Cleanup
        ((ProximumEmbeddingStore) store).close();
    }
}
```

---

## Next Steps

1. **Spring AI Integration**: See `docs/SPRING_AI_GUIDE.md` for Spring Boot examples
2. **Cryptographic Auditability**: Deep-dive into commit hashing
3. **Benchmarks**: See `BENCHMARK_RESULTS.md` for performance comparisons
4. **Commercial Features**: Prometheus metrics export

---

## References

- [LangChain4j Documentation](https://docs.langchain4j.dev/)
- [LangChain4j GitHub](https://github.com/langchain4j/langchain4j)
- [LangChain4j Tutorials](https://docs.langchain4j.dev/tutorials/)
- [Proximum GitHub](https://github.com/replikativ/proximum)
- [Integration Test Example](/test/java/org/replikativ/proximum/langchain4j/LangChain4jIntegrationTest.java)
