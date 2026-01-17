package org.replikativ.proximum.spring;

import org.replikativ.proximum.DistanceMetric;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for {@link PersistentVectorStoreAutoConfiguration}.
 */
@ConfigurationProperties(prefix = "proximum")
public class PersistentVectorStoreProperties {

    /**
     * Filesystem path used to persist the vector index.
     */
    private String storagePath;

    /**
     * Embedding dimensions. If not set, detected from the EmbeddingModel.
     */
    private Integer dimensions;

    private Integer m;
    private Integer efConstruction;
    private Integer efSearch;
    private Long capacity;
    private DistanceMetric distance;

    public String getStoragePath() {
        return storagePath;
    }

    public void setStoragePath(String storagePath) {
        this.storagePath = storagePath;
    }

    public Integer getDimensions() {
        return dimensions;
    }

    public void setDimensions(Integer dimensions) {
        this.dimensions = dimensions;
    }

    public Integer getM() {
        return m;
    }

    public void setM(Integer m) {
        this.m = m;
    }

    public Integer getEfConstruction() {
        return efConstruction;
    }

    public void setEfConstruction(Integer efConstruction) {
        this.efConstruction = efConstruction;
    }

    public Integer getEfSearch() {
        return efSearch;
    }

    public void setEfSearch(Integer efSearch) {
        this.efSearch = efSearch;
    }

    public Long getCapacity() {
        return capacity;
    }

    public void setCapacity(Long capacity) {
        this.capacity = capacity;
    }

    public DistanceMetric getDistance() {
        return distance;
    }

    public void setDistance(DistanceMetric distance) {
        this.distance = distance;
    }
}
