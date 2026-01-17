package com.example.rag;

import org.replikativ.proximum.SearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Service for document ingestion, search, and version control.
 *
 * <p>Uses Spring AI VectorStore wrapper for automatic embedding generation.
 * For Proximum-specific features (snapshots, branches), uses the core store.
 */
@Service
public class DocumentService {

    private static final Logger log = LoggerFactory.getLogger(DocumentService.class);

    private final VectorStore vectorStore;
    private final org.replikativ.proximum.ProximumVectorStore proximumStore;

    // Track snapshots for version control demo
    private final Map<String, UUID> snapshots = new HashMap<>();

    public DocumentService(VectorStore vectorStore,
                          org.replikativ.proximum.ProximumVectorStore proximumStore) {
        this.vectorStore = vectorStore;
        this.proximumStore = proximumStore;
    }

    /**
     * Add a single document to the vector store.
     */
    public String addDocument(com.example.rag.Document doc) {
        log.info("Adding document: {}", doc.getId());

        // Convert to Spring AI Document (embeddings handled automatically)
        Document springDoc = Document.builder()
                .id(doc.getId())
                .text(doc.getContent())
                .metadata(doc.getMetadata() != null ? doc.getMetadata() : new HashMap<>())
                .build();

        vectorStore.add(List.of(springDoc));

        log.info("Document added: {}", doc.getId());
        return doc.getId();
    }

    /**
     * Add multiple documents in a batch (more efficient).
     */
    public List<String> addDocuments(List<com.example.rag.Document> documents) {
        log.info("Adding {} documents in batch", documents.size());

        // Convert to Spring AI Documents (embeddings handled automatically)
        List<Document> springDocs = documents.stream()
                .map(doc -> Document.builder()
                        .id(doc.getId())
                        .text(doc.getContent())
                        .metadata(doc.getMetadata() != null ? doc.getMetadata() : new HashMap<>())
                        .build())
                .toList();

        vectorStore.add(springDocs);

        List<String> ids = springDocs.stream()
                .map(Document::getId)
                .toList();

        log.info("Batch added {} documents", ids.size());
        return ids;
    }

    /**
     * Search for similar documents.
     */
    public List<Document> search(String query, int k) {
        log.info("Searching for: '{}' (k={})", query, k);

        SearchRequest request = SearchRequest.builder()
                .query(query)
                .topK(k)
                .build();

        List<Document> results = vectorStore.similaritySearch(request);

        log.info("Found {} results", results.size());
        return results;
    }

    /**
     * Search with similarity threshold.
     */
    public List<Document> searchWithThreshold(String query, int k, double threshold) {
        log.info("Searching with threshold: '{}' (k={}, threshold={})", query, k, threshold);

        SearchRequest request = SearchRequest.builder()
                .query(query)
                .topK(k)
                .similarityThreshold(threshold)
                .build();

        List<Document> results = vectorStore.similaritySearch(request);

        log.info("Found {} results", results.size());
        return results;
    }

    /**
     * Create a snapshot (commit) of the current state.
     */
    public UUID createSnapshot(String name) {
        log.info("Creating snapshot: {}", name);

        proximumStore.sync();
        UUID commitId = proximumStore.getCommitId();

        snapshots.put(name, commitId);

        log.info("Snapshot created: {} -> {}", name, commitId);
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
        return proximumStore.getHistory();
    }

    /**
     * Get metrics about the index.
     */
    public Map<String, Object> getMetrics() {
        return proximumStore.getMetrics();
    }

    /**
     * Sync the index to storage.
     */
    public void sync() {
        log.info("Syncing vector store...");
        proximumStore.sync();
        log.info("Sync complete");
    }
}
