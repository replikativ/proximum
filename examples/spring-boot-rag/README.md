# Spring Boot RAG Example with Proximum

> **License**: This example is released under the MIT License - feel free to copy and modify for your own projects!

A complete RAG (Retrieval-Augmented Generation) application demonstrating Proximum's unique versioning and auditability features.

## Features Demonstrated

- **Document Ingestion**: Batch upload with OpenAI embeddings
- **Semantic Search**: Vector similarity search with latency controls
- **Version Control**: Git-like snapshots and branches
- **Time Travel**: Query historical states
- **Auditability**: Cryptographic commit hashing
- **Spring Boot Integration**: Full REST API

## Testing Status

✅ **This example has been tested and verified working with:**
- Spring AI 1.0.0-M8
- Fireworks AI with Llama 3.1 8B Instruct and Nomic embeddings
- Java 25 (Oracle GraalVM)
- Full RAG workflow including document ingestion, semantic search, and LLM chat

All core functionality is operational. Please report any issues you encounter!

## Prerequisites

- **Java 25+** (or Java 21 LTS with `--enable-preview`)
- Maven 3.8+
- OpenAI API key or OpenAI-compatible API (e.g., Fireworks AI)

## Setup

1. **Set API credentials**:
   ```bash
   # For OpenAI
   export OPENAI_API_KEY=sk-...

   # For Fireworks AI (OpenAI-compatible)
   export OPENAI_API_KEY=fw-...
   export OPENAI_BASE_URL=https://api.fireworks.ai/inference
   export OPENAI_MODEL=accounts/fireworks/models/llama-v3p1-8b-instruct
   export OPENAI_EMBEDDING_MODEL=nomic-ai/nomic-embed-text-v1.5
   export OPENAI_EMBEDDING_DIMENSIONS=768
   ```

   **Note**: Spring AI appends `/v1/...` to the base URL, so don't include `/v1` at the end of `OPENAI_BASE_URL`.

2. **Build and Run**:
   ```bash
   cd examples/spring-boot-rag
   mvn spring-boot:run -Dspring-boot.run.jvmArguments="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED"
   ```

   Or build JAR and run:
   ```bash
   mvn clean package
   java --add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED \
        -jar target/spring-boot-rag-0.1.0-SNAPSHOT.jar
   ```

The application starts on http://localhost:8080

## API Endpoints

### Add Documents

```bash
# Add single document (id is required)
curl -X POST http://localhost:8080/api/documents \
  -H "Content-Type: application/json" \
  -d '{
    "id": "doc1",
    "content": "Proximum is a vector database with version control",
    "metadata": {"category": "tech", "source": "docs"}
  }'

# Batch add documents
curl -X POST http://localhost:8080/api/documents/batch \
  -H "Content-Type: application/json" \
  -d '[
    {
      "id": "doc2",
      "content": "Vector databases enable semantic search",
      "metadata": {"category": "tech"}
    },
    {
      "id": "doc3",
      "content": "HNSW graphs provide fast approximate search",
      "metadata": {"category": "algorithms"}
    }
  ]'
```

### Chat (RAG with LLM)

```bash
# Ask questions about your documents (uses RAG: retrieval + generation)
curl -X POST http://localhost:8080/api/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "What is Proximum?"}'

# The endpoint automatically:
# 1. Retrieves relevant documents from the vector store
# 2. Passes them as context to the LLM
# 3. Returns an informed answer
```

### Search

```bash
# Basic search
curl -X POST http://localhost:8080/api/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "how does vector search work?",
    "k": 5
  }'

# Search with options (latency controls)
curl -X POST http://localhost:8080/api/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "vector database features",
    "k": 10,
    "options": {
      "ef": 100,
      "timeout-ms": 50,
      "min-similarity": 0.7
    }
  }'
```

### Version Control

```bash
# Create a snapshot (commit)
curl -X POST http://localhost:8080/api/snapshots \
  -H "Content-Type: application/json" \
  -d '{"name": "v1.0-baseline"}'

# List all snapshots
curl http://localhost:8080/api/snapshots

# Get commit history
curl http://localhost:8080/api/history
```

### Metrics

```bash
# Get index health metrics
curl http://localhost:8080/api/metrics

# Returns:
# {
#   "vector-count": 100,
#   "live-count": 95,
#   "deleted-count": 5,
#   "deletion-ratio": 0.05,
#   "needs-compaction?": false,
#   "branch": "main",
#   "commit-id": "550e8400-...",
#   ...
# }
```

### Sync

```bash
# Manually sync to storage
curl -X POST http://localhost:8080/api/sync
```

## Use Cases Demonstrated

### 1. Semantic Search

```bash
# Add documents about different topics
curl -X POST http://localhost:8080/api/documents/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"content": "Python is a high-level programming language"},
    {"content": "Machine learning models require large datasets"},
    {"content": "Docker containers enable application portability"}
  ]'

# Search semantically
curl -X POST http://localhost:8080/api/search \
  -H "Content-Type: application/json" \
  -d '{"query": "software development tools", "k": 3}'
```

### 2. Version Control Workflow

```bash
# Baseline: Add initial documents
curl -X POST http://localhost:8080/api/documents/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"content": "Product A features"},
    {"content": "Product B specifications"}
  ]'

# Create snapshot
curl -X POST http://localhost:8080/api/snapshots \
  -H "Content-Type: application/json" \
  -d '{"name": "baseline"}'

# Add more documents (new product launch)
curl -X POST http://localhost:8080/api/documents/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"content": "Product C announcement"},
    {"content": "Product C pricing"}
  ]'

# Create another snapshot
curl -X POST http://localhost:8080/api/snapshots \
  -H "Content-Type: application/json" \
  -d '{"name": "after-product-c-launch"}'

# Now you can query "what was in the index before Product C?"
# by loading the baseline snapshot
```

### 3. Auditability

```bash
# With crypto-hash enabled, every commit is cryptographically hashed

# Get history with commit hashes
curl http://localhost:8080/api/history

# Returns:
# [
#   {
#     "proximum/commit-id": "550e8400-...",
#     "proximum/commit-hash": "9d4a2f1c-...",  # SHA-512 based
#     "proximum/created-at": "2024-01-16T...",
#     "proximum/vector-count": 100
#   },
#   ...
# ]

# Commit hashes chain like git - proves data integrity
```

## Configuration

Edit `src/main/resources/application.yml`:

```yaml
proximum:
  storage-path: /path/to/storage
  dimensions: 1536                # OpenAI ada-002
  distance: COSINE                # Best for normalized embeddings
  m: 16                           # HNSW connectivity
  ef-construction: 200            # Build quality
  capacity: 100000                # Max vectors
  crypto-hash: true               # Enable auditability
```

## Architecture

```
┌─────────────────┐
│  RagController  │  REST API endpoints
└────────┬────────┘
         │
┌────────▼────────┐
│ DocumentService │  Business logic
└────────┬────────┘
         │
    ┌────┴────┬──────────────────┐
    │         │                  │
┌───▼──────┐  │  ┌──────────────▼──┐
│ Proximum │  │  │ OpenAI Embedding│
│  Vector  │  │  │      Model      │
│  Store   │  │  └─────────────────┘
└──────────┘  │
              │
        ┌─────▼──────┐
        │  Storage   │  File/RocksDB
        └────────────┘
```

## Testing

```bash
# Run tests
mvn test

# Integration test with actual OpenAI API
mvn verify -Dspring.profiles.active=integration
```

## Deployment

### Docker

```dockerfile
FROM eclipse-temurin:25-jre-alpine
WORKDIR /app
COPY target/spring-boot-rag-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 8080
CMD ["java", \
     "--add-modules=jdk.incubator.vector", \
     "--enable-native-access=ALL-UNNAMED", \
     "-jar", "app.jar"]
```

### Kubernetes

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: proximum-config
data:
  application.yml: |
    proximum:
      storage-path: /data/vectors
      crypto-hash: true
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
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rag-app
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: app
        image: your-registry/spring-boot-rag:0.1.0
        env:
        - name: OPENAI_API_KEY
          valueFrom:
            secretKeyRef:
              name: openai-secret
              key: api-key
        volumeMounts:
        - name: storage
          mountPath: /data/vectors
      volumes:
      - name: storage
        persistentVolumeClaim:
          claimName: proximum-storage
```

## Performance Tips

1. **Batch Operations**: Use `/api/documents/batch` for bulk ingestion
2. **Search Options**: Tune `ef` based on recall requirements
3. **Sync Strategy**: Sync after batches, not after every document
4. **Capacity Planning**: Set `capacity` to expected max vectors

## Troubleshooting

### "ClassNotFoundException: jdk.incubator.vector.VectorSpecies"

Add JVM options: `--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED`

### "OpenAI API key not set"

Set environment variable: `export OPENAI_API_KEY=sk-...`

### Slow searches

Increase `ef` in search options or tune HNSW parameters in config.

### "404 - Path not found: /v1/v1/embeddings"

Spring AI automatically appends `/v1/...` to the base URL. Remove `/v1` from your `OPENAI_BASE_URL`:

```bash
# Wrong:
export OPENAI_BASE_URL=https://api.fireworks.ai/inference/v1

# Correct:
export OPENAI_BASE_URL=https://api.fireworks.ai/inference
```

### "id cannot be null or empty"

Documents require an `id` field when using the Spring AI Document API:

```json
{"id": "doc1", "content": "Your content here"}
```

## Next Steps

- Explore LangChain4j integration (see `docs/LANGCHAIN4J_GUIDE.md`)
- Implement filtered search for multi-tenancy
- Add online compaction for high-deletion workloads
- Export metrics to Prometheus

## License

EPL-2.0
