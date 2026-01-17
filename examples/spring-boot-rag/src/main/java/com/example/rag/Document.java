package com.example.rag;

import java.util.Map;
import java.util.UUID;

/**
 * Represents a document with text content and metadata.
 */
public class Document {

    private String id;
    private String content;
    private Map<String, Object> metadata;

    public Document() {
    }

    public Document(String content) {
        this.id = UUID.randomUUID().toString();
        this.content = content;
    }

    public Document(String id, String content, Map<String, Object> metadata) {
        this.id = id;
        this.content = content;
        this.metadata = metadata;
    }

    // Getters and setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "Document{" +
                "id='" + id + '\'' +
                ", content='" + (content != null && content.length() > 50 ?
                content.substring(0, 50) + "..." : content) + '\'' +
                ", metadata=" + metadata +
                '}';
    }
}
