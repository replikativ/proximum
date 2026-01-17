package com.example.rag;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Spring Boot RAG Application using Proximum Vector Database.
 *
 * <p>Demonstrates key features:
 * - Document ingestion with OpenAI embeddings
 * - Semantic search with versioning
 * - Time-travel queries (query historical state)
 * - Branch/snapshot workflow for experimentation
 * - Cryptographic auditability
 *
 * <p>Run with: mvn spring-boot:run -Dspring-boot.run.jvmArguments="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED"
 */
@SpringBootApplication
@EnableConfigurationProperties(VectorStoreProperties.class)
public class RagApplication {

    public static void main(String[] args) {
        SpringApplication.run(RagApplication.class, args);
    }
}
