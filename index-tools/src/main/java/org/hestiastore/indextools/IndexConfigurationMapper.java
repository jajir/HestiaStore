package org.hestiastore.indextools;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationBuilder;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.WalCorruptionPolicy;
import org.hestiastore.index.segmentindex.WalDurabilityMode;

final class IndexConfigurationMapper {

    private IndexConfigurationMapper() {
    }

    static IndexConfigurationManifest toManifest(
            final IndexConfiguration<?, ?> configuration) {
        final IndexConfigurationManifest manifest = new IndexConfigurationManifest();
        manifest.setIndexName(configuration.getIndexName());
        manifest.setKeyClassName(configuration.getKeyClass().getName());
        manifest.setValueClassName(configuration.getValueClass().getName());
        manifest.setKeyTypeDescriptor(configuration.getKeyTypeDescriptor());
        manifest.setValueTypeDescriptor(configuration.getValueTypeDescriptor());
        manifest.setMaxNumberOfKeysInSegmentCache(
                configuration.getMaxNumberOfKeysInSegmentCache());
        manifest.setMaxNumberOfKeysInActivePartition(
                configuration.getMaxNumberOfKeysInActivePartition());
        manifest.setMaxNumberOfKeysInPartitionBuffer(
                configuration.getMaxNumberOfKeysInPartitionBuffer());
        manifest.setMaxNumberOfImmutableRunsPerPartition(
                configuration.getMaxNumberOfImmutableRunsPerPartition());
        manifest.setMaxNumberOfKeysInIndexBuffer(
                configuration.getMaxNumberOfKeysInIndexBuffer());
        manifest.setMaxNumberOfKeysInSegmentChunk(
                configuration.getMaxNumberOfKeysInSegmentChunk());
        manifest.setMaxNumberOfDeltaCacheFiles(
                configuration.getMaxNumberOfDeltaCacheFiles());
        manifest.setMaxNumberOfKeysInPartitionBeforeSplit(
                configuration.getMaxNumberOfKeysInPartitionBeforeSplit());
        manifest.setMaxNumberOfKeysInSegment(
                configuration.getMaxNumberOfKeysInSegment());
        manifest.setMaxNumberOfSegmentsInCache(
                configuration.getMaxNumberOfSegmentsInCache());
        manifest.setNumberOfSegmentMaintenanceThreads(
                configuration.getNumberOfSegmentMaintenanceThreads());
        manifest.setNumberOfIndexMaintenanceThreads(
                configuration.getNumberOfIndexMaintenanceThreads());
        manifest.setNumberOfRegistryLifecycleThreads(
                configuration.getNumberOfRegistryLifecycleThreads());
        manifest.setIndexBusyBackoffMillis(
                configuration.getIndexBusyBackoffMillis());
        manifest.setIndexBusyTimeoutMillis(
                configuration.getIndexBusyTimeoutMillis());
        manifest.setBackgroundMaintenanceAutoEnabled(
                configuration.isBackgroundMaintenanceAutoEnabled());
        manifest.setBloomFilterNumberOfHashFunctions(
                configuration.getBloomFilterNumberOfHashFunctions());
        manifest.setBloomFilterIndexSizeInBytes(
                configuration.getBloomFilterIndexSizeInBytes());
        manifest.setBloomFilterProbabilityOfFalsePositive(
                configuration.getBloomFilterProbabilityOfFalsePositive());
        manifest.setDiskIoBufferSize(configuration.getDiskIoBufferSize());
        manifest.setContextLoggingEnabled(
                configuration.isContextLoggingEnabled());
        manifest.setWal(toManifest(configuration.getWal()));
        manifest.setEncodingChunkFilters(
                configuration.getEncodingChunkFilterSpecs().stream()
                        .map(IndexConfigurationMapper::toManifest).toList());
        manifest.setDecodingChunkFilters(
                configuration.getDecodingChunkFilterSpecs().stream()
                        .map(IndexConfigurationMapper::toManifest).toList());
        return manifest;
    }

    static IndexConfiguration<?, ?> fromManifest(
            final IndexConfigurationManifest manifest)
            throws ClassNotFoundException {
        final IndexConfigurationBuilder<Object, Object> builder = IndexConfiguration
                .<Object, Object>builder();
        builder.withName(manifest.getIndexName());
        builder.withKeyClass(loadClass(manifest.getKeyClassName()));
        builder.withValueClass(loadClass(manifest.getValueClassName()));
        builder.withKeyTypeDescriptor(manifest.getKeyTypeDescriptor());
        builder.withValueTypeDescriptor(manifest.getValueTypeDescriptor());
        builder.withMaxNumberOfKeysInSegmentCache(
                manifest.getMaxNumberOfKeysInSegmentCache());
        builder.withMaxNumberOfKeysInActivePartition(
                manifest.getMaxNumberOfKeysInActivePartition());
        builder.withMaxNumberOfKeysInPartitionBuffer(
                manifest.getMaxNumberOfKeysInPartitionBuffer());
        builder.withMaxNumberOfImmutableRunsPerPartition(
                manifest.getMaxNumberOfImmutableRunsPerPartition());
        builder.withMaxNumberOfKeysInIndexBuffer(
                manifest.getMaxNumberOfKeysInIndexBuffer());
        builder.withMaxNumberOfKeysInSegmentChunk(
                manifest.getMaxNumberOfKeysInSegmentChunk());
        builder.withMaxNumberOfDeltaCacheFiles(
                manifest.getMaxNumberOfDeltaCacheFiles());
        builder.withMaxNumberOfKeysInPartitionBeforeSplit(
                manifest.getMaxNumberOfKeysInPartitionBeforeSplit());
        builder.withMaxNumberOfKeysInSegment(
                manifest.getMaxNumberOfKeysInSegment());
        builder.withMaxNumberOfSegmentsInCache(
                manifest.getMaxNumberOfSegmentsInCache());
        builder.withNumberOfSegmentMaintenanceThreads(
                manifest.getNumberOfSegmentMaintenanceThreads());
        builder.withNumberOfIndexMaintenanceThreads(
                manifest.getNumberOfIndexMaintenanceThreads());
        builder.withNumberOfRegistryLifecycleThreads(
                manifest.getNumberOfRegistryLifecycleThreads());
        builder.withIndexBusyBackoffMillis(manifest.getIndexBusyBackoffMillis());
        builder.withIndexBusyTimeoutMillis(manifest.getIndexBusyTimeoutMillis());
        builder.withBackgroundMaintenanceAutoEnabled(
                manifest.getBackgroundMaintenanceAutoEnabled());
        builder.withBloomFilterNumberOfHashFunctions(
                manifest.getBloomFilterNumberOfHashFunctions());
        builder.withBloomFilterIndexSizeInBytes(
                manifest.getBloomFilterIndexSizeInBytes());
        builder.withBloomFilterProbabilityOfFalsePositive(
                manifest.getBloomFilterProbabilityOfFalsePositive());
        builder.withDiskIoBufferSizeInBytes(manifest.getDiskIoBufferSize());
        builder.withContextLoggingEnabled(manifest.getContextLoggingEnabled());
        builder.withWal(fromManifest(manifest.getWal()));
        builder.withEncodingFilterSpecs(
                manifest.getEncodingChunkFilters().stream()
                        .map(IndexConfigurationMapper::fromManifest).toList());
        builder.withDecodingFilterSpecs(
                manifest.getDecodingChunkFilters().stream()
                        .map(IndexConfigurationMapper::fromManifest).toList());
        return builder.build();
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
