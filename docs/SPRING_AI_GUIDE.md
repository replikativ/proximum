# Spring AI Integration Guide

Complete guide for using Proximum with Spring AI's VectorStore API.

> **Spring AI Version**: Tested with 0.8.0+ (Spring Boot 3.2+)

## Table of Contents

1. [Quick Start](#quick-start)
2. [Configuration](#configuration)
3. [Basic Operations](#basic-operations)
4. [RAG with ChatClient](#rag-with-chatclient)
5. [Streaming Responses](#streaming-responses)
6. [Document Upload & ETL](#document-upload--etl)
7. [Metadata Filtering](#metadata-filtering)
8. [Version Control Features](#version-control-features)
9. [Production Deployment](#production-deployment)
10. [Observability](#observability)
11. [Troubleshooting](#troubleshooting)

---

## Quick Start

### Dependencies (Maven)

```xml
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.2.1</version>
</parent>

<properties>
    <spring-ai.version>0.8.0</spring-ai.version>
</properties>

<dependencies>
    <!-- Spring AI OpenAI (for embeddings and chat) -->
    <dependency>
        <groupId>org.springframework.ai</groupId>
        <artifactId>spring-ai-openai-spring-boot-starter</artifactId>
        <version>${spring-ai.version}</version>
    </dependency>

    <!-- Proximum Vector Store -->
    <dependency>
        <groupId>org.replikativ</groupId>
        <artifactId>proximum</artifactId>
        <version>LATEST</version>
    </dependency>

    <!-- Spring Boot Web -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>

    <!-- Optional: For PDF parsing -->
    <dependency>
        <groupId>org.springframework.ai</groupId>
        <artifactId>spring-ai-pdf-document-reader</artifactId>
        <version>${spring-ai.version}</version>
    </dependency>

    <!-- Optional: For text splitting -->
    <dependency>
        <groupId>org.springframework.ai</groupId>
        <artifactId>spring-ai-transformers</artifactId>
        <version>${spring-ai.version}</version>
    </dependency>
</dependencies>

<build>
    <plugins>
        <plugin>
            <artifactId>maven-surefire-plugin</artifactId>
            <configuration>
                <argLine>
                    --add-modules=jdk.incubator.vector
                    --enable-native-access=ALL-UNNAMED
                </argLine>
            </configuration>
        </plugin>
        <plugin>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-maven-plugin</artifactId>
        </plugin>
    </plugins>
</build>
```

### Configuration (application.yml)

```yaml
spring:
  ai:
    openai:
      api-key: ${OPENAI_API_KEY}
      chat:
        model: gpt-4
      embedding:
        model: text-embedding-ada-002
        dimensions: 1536

proximum:
  storage-path: ${java.io.tmpdir}/proximum-vectors
  dimensions: 1536
  distance: COSINE
  m: 16
  ef-construction: 200
  capacity: 100000
  crypto-hash: true  # Enable auditability
```

### Fireworks AI Configuration (OpenAI-Compatible)

If using Fireworks AI instead of OpenAI:

```yaml
spring:
  ai:
    openai:
      api-key: ${FIREWORKS_API_KEY}
      base-url: https://api.fireworks.ai/inference/v1
      chat:
        model: accounts/fireworks/models/llama-v3p1-8b-instruct
      embedding:
        model: nomic-ai/nomic-embed-text-v1.5
        dimensions: 768  # Adjust based on model
```

**Note**: Fireworks AI is OpenAI-compatible, so Spring AI's OpenAI integration works seamlessly.

---

## Configuration

### Spring Boot Configuration Class

```java
package com.example.rag.config;

import org.replikativ.proximum.ProximumVectorStore;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.QuestionAnswerAdvisor;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class ProximumConfig {

    @Value("${proximum.storage-path}")
    private String storagePath;

    @Value("${proximum.dimensions}")
    private int dimensions;

    @Value("${proximum.distance:COSINE}")
    private String distance;

    @Value("${proximum.m:16}")
    private int m;

    @Value("${proximum.ef-construction:200}")
    private int efConstruction;

    @Value("${proximum.capacity:100000}")
    private int capacity;

    @Value("${proximum.crypto-hash:true}")
    private boolean cryptoHash;

    @Bean
    public ProximumVectorStore proximumVectorStore(EmbeddingModel embeddingModel) {
        Map<String, Object> config = Map.of(
                "backend", ":file",
                "path", storagePath,
                "distance", distance,
                "m", m,
                "ef-construction", efConstruction,
                "capacity", capacity,
                "crypto-hash", cryptoHash
        );

        return ProximumVectorStore.builder()
                .embeddingModel(embeddingModel)
                .storagePath(storagePath)
                .dimensions(dimensions)
                .distance(ProximumVectorStore.DistanceMetric.valueOf(distance))
                .m(m)
                .efConstruction(efConstruction)
                .capacity(capacity)
                .cryptoHash(cryptoHash)
                .build();
    }

    @Bean
    public ChatClient chatClient(
            ChatModel chatModel,
            ProximumVectorStore vectorStore
    ) {
        return ChatClient.builder(chatModel)
                .defaultAdvisors(
                        QuestionAnswerAdvisor.builder(vectorStore)
                                .searchRequest(SearchRequest.builder()
                                        .topK(5)
                                        .similarityThreshold(0.7)
                                        .build())
                                .build()
                )
                .build();
    }
}
```

---

## Basic Operations

### Adding Documents

#### Single Document

```java
@Service
public class DocumentService {

    private final ProximumVectorStore vectorStore;

    public DocumentService(ProximumVectorStore vectorStore) {
        this.vectorStore = vectorStore;
    }

    public void addDocument(String content, Map<String, Object> metadata) {
        org.springframework.ai.document.Document doc =
            new org.springframework.ai.document.Document(content, metadata);

        vectorStore.add(List.of(doc));
    }
}
```

#### Batch Documents

```java
public void addDocuments(List<String> contents) {
    List<org.springframework.ai.document.Document> documents = contents.stream()
            .map(content -> new org.springframework.ai.document.Document(
                    content,
                    Map.of("source", "batch-import")
            ))
            .collect(Collectors.toList());

    vectorStore.add(documents);
}
```

### Searching Documents

#### Basic Search

```java
public List<org.springframework.ai.document.Document> search(String query) {
    SearchRequest request = SearchRequest.builder()
            .query(query)
            .topK(10)
            .build();

    return vectorStore.similaritySearch(request);
}
```

#### Search with Similarity Threshold

```java
public List<org.springframework.ai.document.Document> searchWithThreshold(
        String query,
        double threshold
) {
    SearchRequest request = SearchRequest.builder()
            .query(query)
            .topK(10)
            .similarityThreshold(threshold)
            .build();

    return vectorStore.similaritySearch(request);
}
```

### Deleting Documents

```java
public void deleteDocument(String documentId) {
    vectorStore.delete(List.of(documentId));
}

public void deleteDocuments(List<String> documentIds) {
    vectorStore.delete(documentIds);
}
```

---

## RAG with ChatClient

Spring AI's **ChatClient** with **QuestionAnswerAdvisor** provides automatic retrieval-augmented generation.

### Simple RAG Service

```java
@Service
public class RagChatService {

    private final ChatClient chatClient;

    public RagChatService(ChatClient chatClient) {
        this.chatClient = chatClient;
    }

    /**
     * Ask a question - retrieval happens automatically via QuestionAnswerAdvisor.
     */
    public String chat(String userMessage) {
        return chatClient.prompt()
                .user(userMessage)
                .call()
                .content();
    }

    /**
     * Chat with system instructions.
     */
    public String chatWithContext(String userMessage, String systemContext) {
        return chatClient.prompt()
                .system(systemContext)
                .user(userMessage)
                .call()
                .content();
    }
}
```

### REST Controller

```java
@RestController
@RequestMapping("/api/chat")
public class ChatController {

    private final RagChatService chatService;

    public ChatController(RagChatService chatService) {
        this.chatService = chatService;
    }

    @PostMapping
    public ResponseEntity<String> chat(@RequestBody Map<String, String> request) {
        String question = request.get("question");
        String answer = chatService.chat(question);
        return ResponseEntity.ok(answer);
    }
}
```

### Example Usage

```bash
# Add documents
curl -X POST http://localhost:8080/api/documents \
  -H "Content-Type: application/json" \
  -d '{
    "content": "Proximum is a vector database with git-like version control",
    "metadata": {"category": "tech"}
  }'

# Ask a question (RAG automatically retrieves relevant docs)
curl -X POST http://localhost:8080/api/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "What is Proximum?"}'

# Response: "Proximum is a vector database that features git-like version control,
# allowing you to create snapshots, branches, and perform time-travel queries..."
```

---

## Streaming Responses

For better UX, stream LLM responses word-by-word using reactive Flux.

### Streaming Service

```java
import reactor.core.publisher.Flux;

@Service
public class StreamingChatService {

    private final ChatClient chatClient;

    public StreamingChatService(ChatClient chatClient) {
        this.chatClient = chatClient;
    }

    /**
     * Stream chat responses token-by-token.
     */
    public Flux<String> streamChat(String userMessage) {
        return chatClient.prompt()
                .user(userMessage)
                .stream()
                .content();
    }

    /**
     * Stream with full response context (includes retrieved documents).
     */
    public Flux<ChatClientResponse> streamChatWithContext(String userMessage) {
        return chatClient.prompt()
                .user(userMessage)
                .stream()
                .chatClientResponse();
    }
}
```

### REST Controller with Streaming

```java
import org.springframework.http.MediaType;

@RestController
@RequestMapping("/api/chat")
public class StreamingChatController {

    private final StreamingChatService streamingService;

    public StreamingChatController(StreamingChatService streamingService) {
        this.streamingService = streamingService;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> streamChat(@RequestParam String question) {
        return streamingService.streamChat(question);
    }
}
```

### Client-Side Usage (JavaScript)

```javascript
const eventSource = new EventSource(
    'http://localhost:8080/api/chat/stream?question=What+is+Proximum'
);

eventSource.onmessage = (event) => {
    console.log('Token:', event.data);
    document.getElementById('response').innerHTML += event.data;
};

eventSource.onerror = () => {
    eventSource.close();
};
```

---

## Document Upload & ETL

Spring AI provides document readers for PDF, DOCX, HTML, and more.

### Dependencies

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-pdf-document-reader</artifactId>
    <version>${spring-ai.version}</version>
</dependency>
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-transformers</artifactId>
    <version>${spring-ai.version}</version>
</dependency>
```

### Document Ingestion Service

```java
import org.springframework.ai.document.DocumentReader;
import org.springframework.ai.reader.pdf.PagePdfDocumentReader;
import org.springframework.ai.transformer.splitter.TokenTextSplitter;
import org.springframework.core.io.Resource;

@Service
public class DocumentIngestionService {

    private final ProximumVectorStore vectorStore;

    public DocumentIngestionService(ProximumVectorStore vectorStore) {
        this.vectorStore = vectorStore;
    }

    /**
     * Ingest a PDF document: parse, split, and store.
     */
    public int ingestPdf(Resource pdfFile) {
        // 1. Parse PDF
        DocumentReader reader = new PagePdfDocumentReader(pdfFile);
        List<org.springframework.ai.document.Document> documents = reader.get();

        // 2. Split into chunks (default: 800 tokens with 200 overlap)
        TokenTextSplitter splitter = new TokenTextSplitter();
        List<org.springframework.ai.document.Document> chunks = splitter.apply(documents);

        // 3. Add to vector store
        vectorStore.add(chunks);

        return chunks.size();
    }

    /**
     * Ingest with custom chunking.
     */
    public int ingestWithCustomChunking(Resource pdfFile, int chunkSize, int overlap) {
        DocumentReader reader = new PagePdfDocumentReader(pdfFile);
        List<org.springframework.ai.document.Document> documents = reader.get();

        TokenTextSplitter splitter = new TokenTextSplitter(chunkSize, overlap);
        List<org.springframework.ai.document.Document> chunks = splitter.apply(documents);

        vectorStore.add(chunks);
        return chunks.size();
    }
}
```

### Upload Controller

```java
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api/documents")
public class UploadController {

    private final DocumentIngestionService ingestionService;

    public UploadController(DocumentIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping("/upload")
    public ResponseEntity<Map<String, Object>> uploadPdf(
            @RequestParam("file") MultipartFile file
    ) throws IOException {
        Resource resource = file.getResource();
        int chunkCount = ingestionService.ingestPdf(resource);

        return ResponseEntity.ok(Map.of(
                "message", "PDF uploaded and indexed",
                "chunks", chunkCount,
                "filename", file.getOriginalFilename()
        ));
    }
}
```

### Example Usage

```bash
# Upload a PDF document
curl -X POST http://localhost:8080/api/documents/upload \
  -F "file=@manual.pdf"

# Response:
# {
#   "message": "PDF uploaded and indexed",
#   "chunks": 42,
#   "filename": "manual.pdf"
# }

# Now ask questions about the PDF content
curl -X POST http://localhost:8080/api/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "What does the manual say about installation?"}'
```

---

## Metadata Filtering

Filter search results based on document metadata.

### Filter Expression Syntax

Spring AI supports portable filter expressions:

- **Comparison**: `==`, `!=`, `<`, `<=`, `>`, `>=`
- **Logical**: `&&` (AND), `||` (OR)
- **Set operations**: `IN`, `NOT IN`

### Adding Documents with Metadata

```java
public void addDocumentsWithMetadata() {
    List<org.springframework.ai.document.Document> documents = List.of(
            new org.springframework.ai.document.Document(
                    "Product A features",
                    Map.of("category", "products", "year", 2024)
            ),
            new org.springframework.ai.document.Document(
                    "Technical documentation",
                    Map.of("category", "docs", "year", 2024)
            ),
            new org.springframework.ai.document.Document(
                    "Product B specifications",
                    Map.of("category", "products", "year", 2025)
            )
    );

    vectorStore.add(documents);
}
```

### Searching with Filters

```java
public List<org.springframework.ai.document.Document> searchProducts(String query) {
    SearchRequest request = SearchRequest.builder()
            .query(query)
            .topK(10)
            .filterExpression("category == 'products'")
            .build();

    return vectorStore.similaritySearch(request);
}

public List<org.springframework.ai.document.Document> searchRecent(String query) {
    SearchRequest request = SearchRequest.builder()
            .query(query)
            .topK(10)
            .filterExpression("year >= 2024")
            .build();

    return vectorStore.similaritySearch(request);
}

public List<org.springframework.ai.document.Document> searchComplex(String query) {
    SearchRequest request = SearchRequest.builder()
            .query(query)
            .topK(10)
            .filterExpression("category IN ['products', 'docs'] && year >= 2024")
            .build();

    return vectorStore.similaritySearch(request);
}
```

### REST API with Filtering

```java
@PostMapping("/search")
public ResponseEntity<List<org.springframework.ai.document.Document>> search(
        @RequestBody SearchRequestDto requestDto
) {
    SearchRequest.Builder builder = SearchRequest.builder()
            .query(requestDto.getQuery())
            .topK(requestDto.getTopK());

    if (requestDto.getFilterExpression() != null) {
        builder.filterExpression(requestDto.getFilterExpression());
    }

    List<org.springframework.ai.document.Document> results =
            vectorStore.similaritySearch(builder.build());

    return ResponseEntity.ok(results);
}

public static class SearchRequestDto {
    private String query;
    private int topK = 10;
    private String filterExpression;

    // Getters and setters
}
```

### Example Usage

```bash
# Search only in 'products' category
curl -X POST http://localhost:8080/api/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "specifications",
    "topK": 5,
    "filterExpression": "category == '\''products'\''"
  }'

# Search recent documents
curl -X POST http://localhost:8080/api/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "features",
    "topK": 5,
    "filterExpression": "year >= 2024"
  }'

# Complex filter
curl -X POST http://localhost:8080/api/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "technical",
    "topK": 5,
    "filterExpression": "category IN ['\''docs'\'', '\''products'\''] && year >= 2024"
  }'
```

---

## Version Control Features

**Proximum's unique differentiator**: Git-like version control for vector indices.

### Creating Snapshots

```java
@Service
public class VersionControlService {

    private final ProximumVectorStore vectorStore;
    private final Map<String, UUID> snapshots = new ConcurrentHashMap<>();

    public VersionControlService(ProximumVectorStore vectorStore) {
        this.vectorStore = vectorStore;
    }

    /**
     * Create a named snapshot of the current state.
     */
    public UUID createSnapshot(String name) {
        vectorStore.sync();  // Persist current state
        UUID commitId = vectorStore.getCommitId();
        snapshots.put(name, commitId);
        return commitId;
    }

    /**
     * Get all snapshots.
     */
    public Map<String, UUID> getSnapshots() {
        return new HashMap<>(snapshots);
    }

    /**
     * Get commit history.
     */
    public List<Map<String, Object>> getHistory() {
        return vectorStore.getHistory();
    }
}
```

### Branching for Experiments

```java
/**
 * Create a branch for safe experimentation.
 */
public ProximumVectorStore createExperimentBranch(String branchName) {
    vectorStore.sync();  // Must sync before branching
    return vectorStore.branch(branchName);
}
```

### Time-Travel Queries

```java
/**
 * Query historical state of the index.
 */
public List<org.springframework.ai.document.Document> queryHistoricalState(
        String query,
        UUID snapshotCommitId
) {
    // Connect to historical commit
    ProximumVectorStore historicalStore = ProximumVectorStore.connectCommit(
            Map.of("backend", ":file", "path", storagePath),
            snapshotCommitId
    );

    SearchRequest request = SearchRequest.builder()
            .query(query)
            .topK(10)
            .build();

    return historicalStore.similaritySearch(request);
}
```

### REST API for Version Control

```java
@RestController
@RequestMapping("/api/versions")
public class VersionController {

    private final VersionControlService versionService;

    public VersionController(VersionControlService versionService) {
        this.versionService = versionService;
    }

    @PostMapping("/snapshots")
    public ResponseEntity<Map<String, Object>> createSnapshot(
            @RequestBody Map<String, String> body
    ) {
        String name = body.get("name");
        UUID commitId = versionService.createSnapshot(name);

        return ResponseEntity.ok(Map.of(
                "name", name,
                "commitId", commitId,
                "message", "Snapshot created"
        ));
    }

    @GetMapping("/snapshots")
    public ResponseEntity<Map<String, UUID>> getSnapshots() {
        return ResponseEntity.ok(versionService.getSnapshots());
    }

    @GetMapping("/history")
    public ResponseEntity<List<Map<String, Object>>> getHistory() {
        return ResponseEntity.ok(versionService.getHistory());
    }
}
```

### Use Case: A/B Testing Retrieval Strategies

```java
@Service
public class ExperimentService {

    private final ProximumVectorStore productionStore;

    public ExperimentService(ProximumVectorStore productionStore) {
        this.productionStore = productionStore;
    }

    /**
     * Compare retrieval quality between production and experiment branch.
     */
    public Map<String, Object> compareRetrievalStrategies(String query) {
        // Baseline: Production retrieval
        SearchRequest prodRequest = SearchRequest.builder()
                .query(query)
                .topK(5)
                .similarityThreshold(0.7)
                .build();
        List<org.springframework.ai.document.Document> prodResults =
                productionStore.similaritySearch(prodRequest);

        // Experiment: Create branch and test different parameters
        productionStore.sync();
        ProximumVectorStore experimentStore = productionStore.branch("experiment");

        SearchRequest expRequest = SearchRequest.builder()
                .query(query)
                .topK(10)  // More results
                .similarityThreshold(0.5)  // Lower threshold
                .build();
        List<org.springframework.ai.document.Document> expResults =
                experimentStore.similaritySearch(expRequest);

        return Map.of(
                "production", Map.of(
                        "count", prodResults.size(),
                        "results", prodResults
                ),
                "experiment", Map.of(
                        "count", expResults.size(),
                        "results", expResults
                )
        );
    }
}
```

### Use Case: Compliance and Auditability

```java
/**
 * Prove what data the AI saw at a specific point in time.
 */
public Map<String, Object> generateComplianceReport(
        String query,
        UUID auditCommitId
) {
    ProximumVectorStore historicalStore = ProximumVectorStore.connectCommit(
            Map.of("backend", ":file", "path", storagePath),
            auditCommitId
    );

    SearchRequest request = SearchRequest.builder()
            .query(query)
            .topK(10)
            .build();

    List<org.springframework.ai.document.Document> results =
            historicalStore.similaritySearch(request);

    List<Map<String, Object>> history = historicalStore.getHistory();

    return Map.of(
            "query", query,
            "commitId", auditCommitId,
            "retrievedDocuments", results,
            "commitHistory", history,
            "commitHash", history.stream()
                    .filter(h -> h.get("proximum/commit-id").equals(auditCommitId))
                    .findFirst()
                    .map(h -> h.get("proximum/commit-hash"))
                    .orElse("N/A")
    );
}
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

# Copy JAR
COPY target/app.jar app.jar

# Expose port
EXPOSE 8080

# Run with required JVM options
CMD ["java", \
     "--add-modules=jdk.incubator.vector", \
     "--enable-native-access=ALL-UNNAMED", \
     "-jar", "app.jar"]
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: proximum-rag-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: proximum-rag
  template:
    metadata:
      labels:
        app: proximum-rag
    spec:
      containers:
      - name: app
        image: your-registry/proximum-rag:0.1.0
        env:
        - name: OPENAI_API_KEY
          valueFrom:
            secretKeyRef:
              name: openai-secret
              key: api-key
        - name: PROXIMUM_STORAGE_PATH
          value: /data/vectors
        volumeMounts:
        - name: vector-storage
          mountPath: /data/vectors
        resources:
          requests:
            memory: "2Gi"
            cpu: "1"
          limits:
            memory: "4Gi"
            cpu: "2"
      volumes:
      - name: vector-storage
        persistentVolumeClaim:
          claimName: proximum-storage
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: proximum-storage
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

### Performance Tuning

```yaml
proximum:
  # HNSW parameters
  m: 16                    # Connectivity (higher = better recall, more memory)
  ef-construction: 200     # Build quality (higher = better quality, slower build)

  # Search parameters (can be overridden per request)
  ef: 50                   # Search quality (higher = better recall, slower search)

  # Capacity
  capacity: 1000000        # Pre-allocate for 1M vectors

  # Auditability
  crypto-hash: true        # Enable for compliance use cases
```

---

## Observability

### Spring Boot Actuator

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
```

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  metrics:
    export:
      prometheus:
        enabled: true
  tracing:
    sampling:
      probability: 1.0
```

### Custom Metrics

```java
@Service
public class MetricsService {

    private final ProximumVectorStore vectorStore;
    private final MeterRegistry meterRegistry;

    public MetricsService(
            ProximumVectorStore vectorStore,
            MeterRegistry meterRegistry
    ) {
        this.vectorStore = vectorStore;
        this.meterRegistry = meterRegistry;

        // Register gauge for vector count
        Gauge.builder("proximum.vector.count", vectorStore, store -> {
            Map<String, Object> metrics = store.getMetrics();
            return ((Number) metrics.get("vector-count")).doubleValue();
        }).register(meterRegistry);

        // Register gauge for deletion ratio
        Gauge.builder("proximum.deletion.ratio", vectorStore, store -> {
            Map<String, Object> metrics = store.getMetrics();
            return ((Number) metrics.get("deletion-ratio")).doubleValue();
        }).register(meterRegistry);
    }
}
```

### Health Check

```java
@Component
public class ProximumHealthIndicator implements HealthIndicator {

    private final ProximumVectorStore vectorStore;

    public ProximumHealthIndicator(ProximumVectorStore vectorStore) {
        this.vectorStore = vectorStore;
    }

    @Override
    public Health health() {
        try {
            Map<String, Object> metrics = vectorStore.getMetrics();

            int vectorCount = ((Number) metrics.get("vector-count")).intValue();
            boolean needsCompaction = (Boolean) metrics.get("needs-compaction?");

            Health.Builder builder = vectorCount > 0 ? Health.up() : Health.unknown();

            return builder
                    .withDetail("vector-count", vectorCount)
                    .withDetail("needs-compaction", needsCompaction)
                    .withDetail("branch", metrics.get("branch"))
                    .build();
        } catch (Exception e) {
            return Health.down().withException(e).build();
        }
    }
}
```

---

## Troubleshooting

### ClassNotFoundException: jdk.incubator.vector.VectorSpecies

**Problem**: Missing Vector API module.

**Solution**: Add JVM options:
```bash
--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED
```

### OpenAI API Key Not Set

**Problem**: `spring.ai.openai.api-key` not configured.

**Solution**: Set environment variable:
```bash
export OPENAI_API_KEY=sk-...
# Or for Fireworks AI
export FIREWORKS_API_KEY=fw-...
```

### Slow Searches

**Problem**: Search latency too high.

**Solutions**:
1. Increase `ef` parameter in SearchRequest
2. Tune HNSW parameters (m, ef-construction)
3. Check deletion ratio - compact if needed

```java
SearchRequest request = SearchRequest.builder()
        .query(query)
        .topK(10)
        .build();

// Note: Spring AI doesn't expose ef directly in SearchRequest
// Use Proximum's searchWithIds() for advanced options:
Map<String, Object> options = Map.of("ef", 100);
List<SearchResult> results = vectorStore.searchWithIds(queryVector, 10, options);
```

### High Memory Usage

**Problem**: Large indices consuming too much memory.

**Solutions**:
1. Proximum uses SoftReference caching - JVM will free memory under pressure
2. Adjust JVM heap: `-Xmx4g`
3. Consider compaction if deletion ratio is high

### Embedding Dimension Mismatch

**Problem**: `IllegalArgumentException: Vector dimension mismatch`

**Solution**: Ensure configuration matches your embedding model:
```yaml
spring:
  ai:
    openai:
      embedding:
        model: text-embedding-ada-002
        dimensions: 1536

proximum:
  dimensions: 1536  # Must match!
```

**Common dimensions**:
- OpenAI ada-002: 1536
- OpenAI text-embedding-3-small: 512 or 1536
- Fireworks nomic-embed-text: 768
- Sentence Transformers: 384 or 768

---

## Complete Example Application

See `examples/spring-boot-rag/` for a full working application demonstrating:

- Spring Boot 3.2 + Spring AI 0.8
- Document ingestion (single + batch)
- Semantic search with OpenAI embeddings
- Version control (snapshots, branches, time-travel)
- REST API
- Docker and Kubernetes deployment
- Comprehensive README with curl examples

Run the example:
```bash
cd examples/spring-boot-rag
export OPENAI_API_KEY=sk-...
mvn spring-boot:run -Dspring-boot.run.jvmArguments="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED"
```

---

## Next Steps

1. **LangChain4j Integration**: See `docs/LANGCHAIN4J_GUIDE.md`
2. **Cryptographic Auditability**: Deep-dive into commit hashing
3. **Benchmarks**: See `BENCHMARK_RESULTS.md` for performance comparisons
4. **Commercial Features**: Prometheus metrics export

---

## References

- [Spring AI Documentation](https://docs.spring.io/spring-ai/reference/)
- [Spring AI VectorStore API](https://docs.spring.io/spring-ai/reference/api/vectordbs.html)
- [Spring AI RAG Guide](https://docs.spring.io/spring-ai/reference/api/retrieval-augmented-generation.html)
- [Spring AI ChatClient](https://docs.spring.io/spring-ai/reference/api/chatclient.html)
- [Spring AI Advisors](https://docs.spring.io/spring-ai/reference/api/advisors.html)
- [Proximum GitHub](https://github.com/replikativ/proximum)
