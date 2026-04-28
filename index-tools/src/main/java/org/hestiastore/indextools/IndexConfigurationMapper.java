package org.hestiastore.indextools;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.WalCorruptionPolicy;
import org.hestiastore.index.segmentindex.WalDurabilityMode;

final class IndexConfigurationMapper {

    private IndexConfigurationMapper() {
    }

    static IndexConfigurationManifest toManifest(
            final IndexConfiguration<?, ?> configuration) {
        final var identity = configuration.identity();
        final var segment = configuration.segment();
        final var writePath = configuration.writePath();
        final var tuning = configuration.runtimeTuning();
        final var maintenance = configuration.maintenance();
        final var bloomFilter = configuration.bloomFilter();
        final var filters = configuration.filters();
        final IndexConfigurationManifest manifest = new IndexConfigurationManifest();
        manifest.setIndexName(identity.name());
        manifest.setKeyClassName(identity.keyClass().getName());
        manifest.setValueClassName(identity.valueClass().getName());
        manifest.setKeyTypeDescriptor(identity.keyTypeDescriptor());
        manifest.setValueTypeDescriptor(identity.valueTypeDescriptor());
        manifest.setMaxNumberOfKeysInSegmentCache(
                segment.cacheKeyLimit());
        manifest.setMaxNumberOfKeysInActivePartition(
                writePath.segmentWriteCacheKeyLimit());
        manifest.setMaxNumberOfKeysInPartitionBuffer(
                writePath.segmentWriteCacheKeyLimitDuringMaintenance());
        manifest.setMaxNumberOfImmutableRunsPerPartition(
                tuning.legacyImmutableRunLimit());
        manifest.setMaxNumberOfKeysInIndexBuffer(
                writePath.indexBufferedWriteKeyLimit());
        manifest.setMaxNumberOfKeysInSegmentChunk(
                segment.chunkKeyLimit());
        manifest.setMaxNumberOfDeltaCacheFiles(
                segment.deltaCacheFileLimit());
        manifest.setMaxNumberOfKeysInPartitionBeforeSplit(
                writePath.segmentSplitKeyThreshold());
        manifest.setMaxNumberOfKeysInSegment(
                segment.maxKeys());
        manifest.setMaxNumberOfSegmentsInCache(
                segment.cachedSegmentLimit());
        manifest.setNumberOfSegmentMaintenanceThreads(
                maintenance.segmentThreads());
        manifest.setNumberOfIndexMaintenanceThreads(
                maintenance.indexThreads());
        manifest.setNumberOfRegistryLifecycleThreads(
                maintenance.registryLifecycleThreads());
        manifest.setIndexBusyBackoffMillis(
                maintenance.busyBackoffMillis());
        manifest.setIndexBusyTimeoutMillis(
                maintenance.busyTimeoutMillis());
        manifest.setBackgroundMaintenanceAutoEnabled(
                maintenance.backgroundAutoEnabled());
        manifest.setBloomFilterNumberOfHashFunctions(
                bloomFilter.hashFunctions());
        manifest.setBloomFilterIndexSizeInBytes(
                bloomFilter.indexSizeBytes());
        manifest.setBloomFilterProbabilityOfFalsePositive(
                bloomFilter.falsePositiveProbability());
        manifest.setDiskIoBufferSize(configuration.io().diskBufferSizeBytes());
        manifest.setContextLoggingEnabled(
                configuration.logging().contextEnabled());
        manifest.setWal(toManifest(configuration.wal()));
        manifest.setEncodingChunkFilters(
                filters.encodingChunkFilterSpecs().stream()
                        .map(IndexConfigurationMapper::toManifest).toList());
        manifest.setDecodingChunkFilters(
                filters.decodingChunkFilterSpecs().stream()
                        .map(IndexConfigurationMapper::toManifest).toList());
        return manifest;
    }

    static IndexConfiguration<?, ?> fromManifest(
            final IndexConfigurationManifest manifest)
            throws ClassNotFoundException {
        final Class<Object> keyClass = loadClass(manifest.getKeyClassName());
        final Class<Object> valueClass = loadClass(
                manifest.getValueClassName());
        return IndexConfiguration.<Object, Object>builder()
                .identity(identity -> identity.name(manifest.getIndexName())
                        .keyClass(keyClass)
                        .valueClass(valueClass)
                        .keyTypeDescriptor(manifest.getKeyTypeDescriptor())
                        .valueTypeDescriptor(
                                manifest.getValueTypeDescriptor()))
                .segment(segment -> segment
                        .cacheKeyLimit(
                                manifest.getMaxNumberOfKeysInSegmentCache())
                        .chunkKeyLimit(
                                manifest.getMaxNumberOfKeysInSegmentChunk())
                        .deltaCacheFileLimit(
                                manifest.getMaxNumberOfDeltaCacheFiles())
                        .maxKeys(manifest.getMaxNumberOfKeysInSegment())
                        .cachedSegmentLimit(
                                manifest.getMaxNumberOfSegmentsInCache()))
                .writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(
                                manifest.getMaxNumberOfKeysInActivePartition())
                        .maintenanceWriteCacheKeyLimit(
                                manifest.getMaxNumberOfKeysInPartitionBuffer())
                        .legacyImmutableRunLimit(manifest
                                .getMaxNumberOfImmutableRunsPerPartition())
                        .indexBufferedWriteKeyLimit(
                                manifest.getMaxNumberOfKeysInIndexBuffer())
                        .segmentSplitKeyThreshold(manifest
                                .getMaxNumberOfKeysInPartitionBeforeSplit()))
                .maintenance(maintenance -> maintenance
                        .segmentThreads(
                                manifest.getNumberOfSegmentMaintenanceThreads())
                        .indexThreads(
                                manifest.getNumberOfIndexMaintenanceThreads())
                        .registryLifecycleThreads(manifest
                                .getNumberOfRegistryLifecycleThreads())
                        .busyBackoffMillis(manifest.getIndexBusyBackoffMillis())
                        .busyTimeoutMillis(manifest.getIndexBusyTimeoutMillis())
                        .backgroundAutoEnabled(
                                manifest.getBackgroundMaintenanceAutoEnabled()))
                .bloomFilter(bloomFilter -> bloomFilter
                        .hashFunctions(
                                manifest.getBloomFilterNumberOfHashFunctions())
                        .indexSizeBytes(
                                manifest.getBloomFilterIndexSizeInBytes())
                        .falsePositiveProbability(manifest
                                .getBloomFilterProbabilityOfFalsePositive()))
                .io(io -> io.diskBufferSizeBytes(manifest.getDiskIoBufferSize()))
                .logging(logging -> logging
                        .contextEnabled(manifest.getContextLoggingEnabled()))
                .wal(wal -> wal.configuration(fromManifest(manifest.getWal())))
                .filters(filters -> filters
                        .encodingFilterSpecs(
                                manifest.getEncodingChunkFilters().stream()
                                        .map(IndexConfigurationMapper::fromManifest)
                                        .toList())
                        .decodingFilterSpecs(
                                manifest.getDecodingChunkFilters().stream()
                                        .map(IndexConfigurationMapper::fromManifest)
                                        .toList()))
                .build();
    }

    private static ChunkFilterSpecManifest toManifest(final ChunkFilterSpec spec) {
        final ChunkFilterSpecManifest manifest = new ChunkFilterSpecManifest();
        manifest.setProviderId(spec.getProviderId());
        manifest.setParameters(spec.getParameters());
        return manifest;
    }

    private static ChunkFilterSpec fromManifest(
            final ChunkFilterSpecManifest manifest) {
        ChunkFilterSpec spec = ChunkFilterSpec
                .ofProvider(manifest.getProviderId());
        for (final var entry : manifest.getParameters().entrySet()) {
            spec = spec.withParameter(entry.getKey(), entry.getValue());
        }
        return spec;
    }

    private static WalManifest toManifest(final Wal wal) {
        final WalManifest manifest = new WalManifest();
        manifest.setEnabled(wal.isEnabled());
        manifest.setDurabilityMode(wal.getDurabilityMode().name());
        manifest.setSegmentSizeBytes(wal.getSegmentSizeBytes());
        manifest.setGroupSyncDelayMillis(wal.getGroupSyncDelayMillis());
        manifest.setGroupSyncMaxBatchBytes(wal.getGroupSyncMaxBatchBytes());
        manifest.setMaxBytesBeforeForcedCheckpoint(
                wal.getMaxBytesBeforeForcedCheckpoint());
        manifest.setCorruptionPolicy(wal.getCorruptionPolicy().name());
        manifest.setEpochSupport(wal.isEpochSupport());
        return manifest;
    }

    private static Wal fromManifest(final WalManifest manifest) {
        if (manifest == null || !manifest.isEnabled()) {
            return Wal.EMPTY;
        }
        return Wal.builder()
                .withDurabilityMode(
                        WalDurabilityMode.valueOf(manifest.getDurabilityMode()))
                .withSegmentSizeBytes(manifest.getSegmentSizeBytes())
                .withGroupSyncDelayMillis(manifest.getGroupSyncDelayMillis())
                .withGroupSyncMaxBatchBytes(
                        manifest.getGroupSyncMaxBatchBytes())
                .withMaxBytesBeforeForcedCheckpoint(
                        manifest.getMaxBytesBeforeForcedCheckpoint())
                .withCorruptionPolicy(WalCorruptionPolicy
                        .valueOf(manifest.getCorruptionPolicy()))
                .withEpochSupport(manifest.isEpochSupport()).build();
    }

    @SuppressWarnings("unchecked")
    private static Class<Object> loadClass(final String className)
            throws ClassNotFoundException {
        return (Class<Object>) Class.forName(className);
    }

    static IndexConfigurationManifest withIndexName(
            final IndexConfigurationManifest manifest, final String indexName) {
        final IndexConfigurationManifest copy = ManifestSupport.mapper()
                .convertValue(manifest, IndexConfigurationManifest.class);
        copy.setIndexName(indexName);
        return copy;
    }

    static List<String> describeImmutableFields() {
        return List.of("keyClassName", "valueClassName", "keyTypeDescriptor",
                "valueTypeDescriptor", "encodingChunkFilters",
                "decodingChunkFilters", "maxNumberOfKeysInSegmentChunk",
                "maxNumberOfKeysInSegment", "bloomFilterNumberOfHashFunctions",
                "bloomFilterIndexSizeInBytes",
                "bloomFilterProbabilityOfFalsePositive");
    }
}
