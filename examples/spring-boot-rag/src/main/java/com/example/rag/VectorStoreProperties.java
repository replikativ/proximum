package com.example.rag;

import org.replikativ.proximum.DistanceMetric;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "proximum")
public class VectorStoreProperties {

    private String storagePath;
    private int dimensions = 1536;
    private DistanceMetric distance = DistanceMetric.COSINE;
    private int m = 16;
    private int efConstruction = 200;
    private int capacity = 100000;
    private boolean cryptoHash = false;

    // Getters and setters

    public String getStoragePath() {
        return storagePath;
    }

    public void setStoragePath(String storagePath) {
        this.storagePath = storagePath;
    }

    public int getDimensions() {
        return dimensions;
    }

    public void setDimensions(int dimensions) {
        this.dimensions = dimensions;
    }

    public DistanceMetric getDistance() {
        return distance;
    }

    public void setDistance(DistanceMetric distance) {
        this.distance = distance;
    }

    public int getM() {
        return m;
    }

    public void setM(int m) {
        this.m = m;
    }

    public int getEfConstruction() {
        return efConstruction;
    }

    public void setEfConstruction(int efConstruction) {
        this.efConstruction = efConstruction;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public boolean isCryptoHash() {
        return cryptoHash;
    }

    public void setCryptoHash(boolean cryptoHash) {
        this.cryptoHash = cryptoHash;
    }
}
