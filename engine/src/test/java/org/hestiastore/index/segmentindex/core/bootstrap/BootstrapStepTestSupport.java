package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.properties.IndexPropertiesSchema;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfigurationBuilder;
import org.hestiastore.index.segmentindex.configuration.user.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStorage;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;

final class BootstrapStepTestSupport {

    static final String LOCK_FILE_NAME = ".lock";
    static final String CONFIGURATION_FILE_NAME =
            IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME;

    private BootstrapStepTestSupport() {
    }

    static IndexConfiguration<Integer, String> configuration(
            final String indexName) {
        return configuration(indexName, false);
    }

    static IndexConfiguration<Integer, String> configuration(
            final String indexName, final boolean contextLoggingEnabled) {
        return configuration(indexName, contextLoggingEnabled, 1);
    }

    static IndexConfiguration<Integer, String> configuration(
            final String indexName, final boolean contextLoggingEnabled,
            final int registryLifecycleThreads) {
        return configuration(indexName, contextLoggingEnabled,
                registryLifecycleThreads, null);
    }

    static IndexConfiguration<Integer, String> configurationWithWal(
            final String indexName) {
        return configuration(indexName, false, 1,
                IndexWalConfiguration.builder().build());
    }

    private static IndexConfiguration<Integer, String> configuration(
            final String indexName, final boolean contextLoggingEnabled,
            final int registryLifecycleThreads,
            final IndexWalConfiguration walConfiguration) {
        final IndexConfigurationBuilder<Integer, String> builder =
                IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity
                        .keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity
                        .valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name(indexName))
                .logging(logging -> logging
                        .contextEnabled(contextLoggingEnabled))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath
                        .maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter
                        .falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .maintenance(maintenance -> maintenance
                        .backgroundAutoEnabled(false))
                .maintenance(maintenance -> maintenance.segmentThreads(1))
                .maintenance(maintenance -> maintenance
                        .registryLifecycleThreads(registryLifecycleThreads))
                .filters(filters -> filters.encodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(
                        List.of(new ChunkFilterDoNothing())));
        if (walConfiguration != null) {
            builder.wal(wal -> wal.configuration(walConfiguration));
        }
        return builder.build();
    }

    static EffectiveIndexConfiguration<Integer, String> effectiveConfiguration(
            final String indexName) {
        return effectiveConfiguration(configuration(indexName));
    }

    static EffectiveIndexConfiguration<Integer, String> effectiveConfiguration(
            final IndexConfiguration<Integer, String> configuration) {
        return EffectiveIndexConfigurationTestSupport.effective(configuration);
    }

    static SegmentIndexBootstrapRequest<Integer, String> request(
            final Directory directory, final SegmentIndexBootstrapMode mode) {
        return request(directory, configuration("bootstrap-step-test"), mode);
    }

    static SegmentIndexBootstrapRequest<Integer, String> request(
            final Directory directory,
            final IndexConfiguration<Integer, String> configuration,
            final SegmentIndexBootstrapMode mode) {
        return new SegmentIndexBootstrapRequest<>(directory, configuration,
                null, mode);
    }

    static SegmentIndexBootstrapState<Integer, String> stateWithConfiguration(
            final EffectiveIndexConfiguration<Integer, String> configuration) {
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();
        state.setConfiguration(configuration);
        return state;
    }

    static SegmentIndexBootstrapState<Integer, String> stateWithRuntimeInputs(
            final EffectiveIndexConfiguration<Integer, String> configuration,
            final ExecutorRegistry executorRegistry) {
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithConfiguration(configuration);
        state.setKeyTypeDescriptor(new TypeDescriptorInteger());
        state.setValueTypeDescriptor(new TypeDescriptorShortString());
        state.setExecutorRegistry(executorRegistry);
        return state;
    }

    static void saveConfiguration(final Directory directory,
            final IndexConfiguration<Integer, String> configuration) {
        new IndexConfigurationStorage<Integer, String>(directory)
                .save(effectiveConfiguration(configuration));
    }

    static ExecutorRegistry executorRegistry(
            final EffectiveIndexConfiguration<Integer, String> configuration) {
        return ExecutorRegistryFixture.from(configuration);
    }

    static <K, V> void closeRuntimePreparationResources(
            final SegmentIndexBootstrapState<K, V> state) {
        RuntimeException failure = null;
        if (state.hasRuntimeSplitService()) {
            failure = closeIgnoringFailure(
                    state.getRuntimeSplitService()::close, failure);
        }
        if (state.hasCoreStorage()) {
            final SegmentIndexCoreStorage<K, V> coreStorage =
                    state.getCoreStorage();
            if (!coreStorage.wasClosed()) {
                failure = closeIgnoringFailure(coreStorage::close, failure);
            }
        }
        if (state.hasRuntimeWalRuntime()) {
            failure = closeIgnoringFailure(state.getRuntimeWalRuntime()::close,
                    failure);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException closeIgnoringFailure(
            final Runnable closeAction, final RuntimeException failure) {
        try {
            closeAction.run();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            if (failure == null) {
                return cleanupFailure;
            }
            failure.addSuppressed(cleanupFailure);
            return failure;
        }
    }
}
