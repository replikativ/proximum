package com.example.rag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.document.Document;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST API for RAG operations.
 *
 * <p>Endpoints:
 * - POST /api/documents - Add documents
 * - POST /api/search - Search for similar documents
 * - POST /api/snapshots - Create snapshot
 * - GET /api/snapshots - List snapshots
 * - GET /api/metrics - Get index metrics
 * - GET /api/history - Get commit history
 */
@RestController
@RequestMapping("/api")
public class RagController {

    private static final Logger log = LoggerFactory.getLogger(RagController.class);

    private final DocumentService documentService;
    private final ChatClient chatClient;

    public RagController(DocumentService documentService, ChatClient chatClient) {
        this.documentService = documentService;
        this.chatClient = chatClient;
    }

    /**
     * Add a single document.
     */
    @PostMapping("/documents")
    public ResponseEntity<Map<String, Object>> addDocument(@RequestBody com.example.rag.Document document) {
        String id = documentService.addDocument(document);
        return ResponseEntity.ok(Map.of(
                "id", id,
                "message", "Document added successfully"
        ));
    }

    /**
     * Add multiple documents in batch.
     */
    @PostMapping("/documents/batch")
    public ResponseEntity<Map<String, Object>> addDocuments(@RequestBody List<com.example.rag.Document> documents) {
        List<String> ids = documentService.addDocuments(documents);
        return ResponseEntity.ok(Map.of(
                "ids", ids,
                "count", ids.size(),
                "message", "Documents added successfully"
        ));
    }

    /**
     * Search for similar documents.
     */
    @PostMapping("/search")
    public ResponseEntity<Map<String, Object>> search(@RequestBody SearchRequest request) {
        List<Document> results;

        if (request.getThreshold() != null && request.getThreshold() > 0) {
            results = documentService.searchWithThreshold(
                    request.getQuery(),
                    request.getK(),
                    request.getThreshold()
            );
        } else {
            results = documentService.search(request.getQuery(), request.getK());
        }

        return ResponseEntity.ok(Map.of(
                "query", request.getQuery(),
                "k", request.getK(),
                "results", results
        ));
    }

    /**
     * Create a snapshot of the current state.
     */
    @PostMapping("/snapshots")
    public ResponseEntity<Map<String, Object>> createSnapshot(@RequestBody Map<String, String> body) {
        String name = body.get("name");
        UUID commitId = documentService.createSnapshot(name);

        return ResponseEntity.ok(Map.of(
                "name", name,
                "commitId", commitId,
                "message", "Snapshot created successfully"
        ));
    }

    /**
     * List all snapshots.
     */
    @GetMapping("/snapshots")
    public ResponseEntity<Map<String, UUID>> getSnapshots() {
        return ResponseEntity.ok(documentService.getSnapshots());
    }

    /**
     * Get index metrics.
     */
    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        return ResponseEntity.ok(documentService.getMetrics());
    }

    /**
     * Get commit history.
     */
    @GetMapping("/history")
    public ResponseEntity<List<Map<String, Object>>> getHistory() {
        return ResponseEntity.ok(documentService.getHistory());
    }

    /**
     * Sync the index to storage.
     */
    @PostMapping("/sync")
    public ResponseEntity<Map<String, Object>> sync() {
        documentService.sync();
        return ResponseEntity.ok(Map.of("message", "Index synced successfully"));
    }

    /**
     * Health check endpoint.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }

    /**
     * Chat endpoint - RAG with LLM answer generation.
     */
    @PostMapping("/chat")
    public ResponseEntity<Map<String, String>> chat(@RequestBody Map<String, String> request) {
        String question = request.get("question");
        log.info("Chat request: {}", question);

        String answer = chatClient.prompt()
                .user(question)
                .call()
                .content();

        return ResponseEntity.ok(Map.of(
                "question", question,
                "answer", answer
        ));
    }

    // Request model
    public static class SearchRequest {
        private String query;
        private int k = 10;
        private Double threshold;

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public int getK() {
            return k;
        }

        public void setK(int k) {
            this.k = k;
        }

        public Double getThreshold() {
            return threshold;
        }

        public void setThreshold(Double threshold) {
            this.threshold = threshold;
        }
    }
}

