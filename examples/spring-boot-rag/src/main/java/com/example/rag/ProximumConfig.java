package com.example.rag;

import org.replikativ.proximum.DistanceMetric;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for Proximum VectorStore with Spring AI integration.
 */
@Configuration
public class ProximumConfig {

    @Value("${proximum.storage-path}")
    private String storagePath;

    @Value("${proximum.dimensions}")
    private int dimensions;

    @Value("${proximum.m:16}")
    private int m;

    @Value("${proximum.ef-construction:200}")
    private int efConstruction;

    @Value("${proximum.capacity:100000}")
    private int capacity;

    @Value("${proximum.distance:COSINE}")
    private String distance;

    @Bean
    public VectorStore vectorStore(EmbeddingModel embeddingModel) {
        return org.replikativ.proximum.spring.ProximumVectorStore.builder()
                .embeddingModel(embeddingModel)
                .storagePath(storagePath)
                .dimensions(dimensions)
                .m(m)
                .efConstruction(efConstruction)
                .capacity(capacity)
                .distance(DistanceMetric.valueOf(distance))
                .build();
    }

    /**
     * Expose the underlying Proximum store for advanced features.
     */
    @Bean
    public org.replikativ.proximum.ProximumVectorStore proximumStore(VectorStore vectorStore) {
        return ((org.replikativ.proximum.spring.ProximumVectorStore) vectorStore).getStore();
    }
}
