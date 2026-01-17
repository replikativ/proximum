package org.replikativ.proximum.spring;

import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot auto-configuration for {@link ProximumVectorStore}.
 *
 * <p>Activated when {@code proximum.storage-path} is set.
 */
@AutoConfiguration
@ConditionalOnClass({ VectorStore.class, EmbeddingModel.class, ProximumVectorStore.class })
@EnableConfigurationProperties(ProximumVectorStoreProperties.class)
public class ProximumVectorStoreAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(VectorStore.class)
    @ConditionalOnProperty(prefix = "proximum", name = "storage-path")
    public VectorStore persistentVectorStore(EmbeddingModel embeddingModel, ProximumVectorStoreProperties props) {
        ProximumVectorStore.Builder builder = ProximumVectorStore.builder()
            .embeddingModel(embeddingModel)
            .storagePath(props.getStoragePath());

        if (props.getDimensions() != null && props.getDimensions() > 0) {
            builder.dimensions(props.getDimensions());
        }
        if (props.getM() != null) {
            builder.m(props.getM());
        }
        if (props.getEfConstruction() != null) {
            builder.efConstruction(props.getEfConstruction());
        }
        if (props.getEfSearch() != null) {
            builder.efSearch(props.getEfSearch());
        }
        if (props.getCapacity() != null) {
            builder.capacity(props.getCapacity());
        }
        if (props.getDistance() != null) {
            builder.distance(props.getDistance());
        }

        return builder.build();
    }
}
