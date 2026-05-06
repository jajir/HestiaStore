package org.hestiastore.indextools;

import java.util.ArrayList;
import java.util.List;

class IndexConfigurationManifest {

    private String indexName;
    private String keyClassName;
    private String valueClassName;
    private String keyTypeDescriptor;
    private String valueTypeDescriptor;
    private Integer maxNumberOfKeysInSegmentCache;
    private Integer segmentWriteCacheKeyLimit;
    private Integer segmentWriteCacheKeyLimitDuringMaintenance;
    private Integer indexBufferedWriteKeyLimit;
    private Integer maxNumberOfKeysInSegmentChunk;
    private Integer maxNumberOfDeltaCacheFiles;
    private Integer segmentSplitKeyThreshold;
    private Integer maxNumberOfKeysInSegment;
    private Integer maxNumberOfSegmentsInCache;
    private Integer numberOfSegmentMaintenanceThreads;
    private Integer numberOfIndexMaintenanceThreads;
    private Integer numberOfRegistryLifecycleThreads;
    private Integer indexBusyBackoffMillis;
    private Integer indexBusyTimeoutMillis;
    private Boolean backgroundMaintenanceAutoEnabled;
    private Integer bloomFilterNumberOfHashFunctions;
    private Integer bloomFilterIndexSizeInBytes;
    private Double bloomFilterProbabilityOfFalsePositive;
    private Integer diskIoBufferSize;
    private Boolean contextLoggingEnabled;
    private WalManifest wal;
    private List<ChunkFilterSpecManifest> encodingChunkFilters = new ArrayList<>();
    private List<ChunkFilterSpecManifest> decodingChunkFilters = new ArrayList<>();

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(final String indexName) {
        this.indexName = indexName;
    }

    public String getKeyClassName() {
        return keyClassName;
    }

    public void setKeyClassName(final String keyClassName) {
        this.keyClassName = keyClassName;
    }

    public String getValueClassName() {
        return valueClassName;
    }

    public void setValueClassName(final String valueClassName) {
        this.valueClassName = valueClassName;
    }

    public String getKeyTypeDescriptor() {
        return keyTypeDescriptor;
    }

    public void setKeyTypeDescriptor(final String keyTypeDescriptor) {
        this.keyTypeDescriptor = keyTypeDescriptor;
    }

    public String getValueTypeDescriptor() {
        return valueTypeDescriptor;
    }

    public void setValueTypeDescriptor(final String valueTypeDescriptor) {
        this.valueTypeDescriptor = valueTypeDescriptor;
    }

    public Integer getMaxNumberOfKeysInSegmentCache() {
        return maxNumberOfKeysInSegmentCache;
    }

    public void setMaxNumberOfKeysInSegmentCache(
            final Integer maxNumberOfKeysInSegmentCache) {
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
    }

    public Integer getSegmentWriteCacheKeyLimit() {
        return segmentWriteCacheKeyLimit;
    }

    public void setSegmentWriteCacheKeyLimit(
            final Integer segmentWriteCacheKeyLimit) {
        this.segmentWriteCacheKeyLimit = segmentWriteCacheKeyLimit;
    }

    public Integer getSegmentWriteCacheKeyLimitDuringMaintenance() {
        return segmentWriteCacheKeyLimitDuringMaintenance;
    }

    public void setSegmentWriteCacheKeyLimitDuringMaintenance(
            final Integer segmentWriteCacheKeyLimitDuringMaintenance) {
        this.segmentWriteCacheKeyLimitDuringMaintenance =
                segmentWriteCacheKeyLimitDuringMaintenance;
    }

    public Integer getIndexBufferedWriteKeyLimit() {
        return indexBufferedWriteKeyLimit;
    }

    public void setIndexBufferedWriteKeyLimit(
            final Integer indexBufferedWriteKeyLimit) {
        this.indexBufferedWriteKeyLimit = indexBufferedWriteKeyLimit;
    }

    public Integer getMaxNumberOfKeysInSegmentChunk() {
        return maxNumberOfKeysInSegmentChunk;
    }

    public void setMaxNumberOfKeysInSegmentChunk(
            final Integer maxNumberOfKeysInSegmentChunk) {
        this.maxNumberOfKeysInSegmentChunk = maxNumberOfKeysInSegmentChunk;
    }

    public Integer getMaxNumberOfDeltaCacheFiles() {
        return maxNumberOfDeltaCacheFiles;
    }

    public void setMaxNumberOfDeltaCacheFiles(
            final Integer maxNumberOfDeltaCacheFiles) {
        this.maxNumberOfDeltaCacheFiles = maxNumberOfDeltaCacheFiles;
    }

    public Integer getSegmentSplitKeyThreshold() {
        return segmentSplitKeyThreshold;
    }

    public void setSegmentSplitKeyThreshold(
            final Integer segmentSplitKeyThreshold) {
        this.segmentSplitKeyThreshold = segmentSplitKeyThreshold;
    }

    public Integer getMaxNumberOfKeysInSegment() {
        return maxNumberOfKeysInSegment;
    }

    public void setMaxNumberOfKeysInSegment(
            final Integer maxNumberOfKeysInSegment) {
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
    }

    public Integer getMaxNumberOfSegmentsInCache() {
        return maxNumberOfSegmentsInCache;
    }

    public void setMaxNumberOfSegmentsInCache(
            final Integer maxNumberOfSegmentsInCache) {
        this.maxNumberOfSegmentsInCache = maxNumberOfSegmentsInCache;
    }

    public Integer getNumberOfSegmentMaintenanceThreads() {
        return numberOfSegmentMaintenanceThreads;
    }

    public void setNumberOfSegmentMaintenanceThreads(
            final Integer numberOfSegmentMaintenanceThreads) {
        this.numberOfSegmentMaintenanceThreads = numberOfSegmentMaintenanceThreads;
    }

    public Integer getNumberOfIndexMaintenanceThreads() {
        return numberOfIndexMaintenanceThreads;
    }

    public void setNumberOfIndexMaintenanceThreads(
            final Integer numberOfIndexMaintenanceThreads) {
        this.numberOfIndexMaintenanceThreads = numberOfIndexMaintenanceThreads;
    }

    public Integer getNumberOfRegistryLifecycleThreads() {
        return numberOfRegistryLifecycleThreads;
    }

    public void setNumberOfRegistryLifecycleThreads(
            final Integer numberOfRegistryLifecycleThreads) {
        this.numberOfRegistryLifecycleThreads = numberOfRegistryLifecycleThreads;
    }

    public Integer getIndexBusyBackoffMillis() {
        return indexBusyBackoffMillis;
    }

    public void setIndexBusyBackoffMillis(final Integer indexBusyBackoffMillis) {
        this.indexBusyBackoffMillis = indexBusyBackoffMillis;
    }

    public Integer getIndexBusyTimeoutMillis() {
        return indexBusyTimeoutMillis;
    }

    public void setIndexBusyTimeoutMillis(final Integer indexBusyTimeoutMillis) {
        this.indexBusyTimeoutMillis = indexBusyTimeoutMillis;
    }

    public Boolean getBackgroundMaintenanceAutoEnabled() {
        return backgroundMaintenanceAutoEnabled;
    }

    public void setBackgroundMaintenanceAutoEnabled(
            final Boolean backgroundMaintenanceAutoEnabled) {
        this.backgroundMaintenanceAutoEnabled = backgroundMaintenanceAutoEnabled;
    }

    public Integer getBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions;
    }

    public void setBloomFilterNumberOfHashFunctions(
            final Integer bloomFilterNumberOfHashFunctions) {
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
    }

    public Integer getBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes;
    }

    public void setBloomFilterIndexSizeInBytes(
            final Integer bloomFilterIndexSizeInBytes) {
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
    }

    public Double getBloomFilterProbabilityOfFalsePositive() {
        return bloomFilterProbabilityOfFalsePositive;
    }

    public void setBloomFilterProbabilityOfFalsePositive(
            final Double bloomFilterProbabilityOfFalsePositive) {
        this.bloomFilterProbabilityOfFalsePositive = bloomFilterProbabilityOfFalsePositive;
    }

    public Integer getDiskIoBufferSize() {
        return diskIoBufferSize;
    }

    public void setDiskIoBufferSize(final Integer diskIoBufferSize) {
        this.diskIoBufferSize = diskIoBufferSize;
    }

    public Boolean getContextLoggingEnabled() {
        return contextLoggingEnabled;
    }

    public void setContextLoggingEnabled(final Boolean contextLoggingEnabled) {
        this.contextLoggingEnabled = contextLoggingEnabled;
    }

    public WalManifest getWal() {
        return wal;
    }

    public void setWal(final WalManifest wal) {
        this.wal = wal;
    }

    public List<ChunkFilterSpecManifest> getEncodingChunkFilters() {
        return encodingChunkFilters;
    }

    public void setEncodingChunkFilters(
            final List<ChunkFilterSpecManifest> encodingChunkFilters) {
        this.encodingChunkFilters = encodingChunkFilters == null
                ? new ArrayList<>()
                : new ArrayList<>(encodingChunkFilters);
    }

    public List<ChunkFilterSpecManifest> getDecodingChunkFilters() {
        return decodingChunkFilters;
    }

    public void setDecodingChunkFilters(
            final List<ChunkFilterSpecManifest> decodingChunkFilters) {
        this.decodingChunkFilters = decodingChunkFilters == null
                ? new ArrayList<>()
                : new ArrayList<>(decodingChunkFilters);
    }
}
