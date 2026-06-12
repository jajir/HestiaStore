package org.hestiastore.index.segmentindex.core.session;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.junit.jupiter.api.Test;

class SegmentIndexSessionFactoryTest {

    @Test
    void createIndex_rejectsNullResources() {
        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexSessionFactory.createIndex(null,
                        effectiveConfiguration("factory-null-resources"),
                        new TypeDescriptorInteger()));
    }

    @Test
    void createIndex_rejectsNullConfiguration() {
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final SegmentIndexSessionResources<Integer, String> resources =
                initializedResources(keyDescriptor,
                        SegmentIndexSessionInfrastructure.create());

        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexSessionFactory.createIndex(resources,
                        null, keyDescriptor));
    }

    @Test
    void createIndex_rejectsNullKeyTypeDescriptor() {
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final SegmentIndexSessionResources<Integer, String> resources =
                initializedResources(keyDescriptor,
                        SegmentIndexSessionInfrastructure.create());

        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexSessionFactory.createIndex(resources,
                        effectiveConfiguration("factory-null-key"), null));
    }

    @Test
    void createIndex_usesInitializedRuntimeAndStateMachine() {
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final SegmentIndexSessionInfrastructure<Integer, String> infrastructure =
                SegmentIndexSessionInfrastructure.create();
        final SegmentIndexRuntime<Integer, String> runtime =
                newRuntime(keyDescriptor);
        final SegmentIndexSessionResources<Integer, String> resources =
                initializedResources(infrastructure, runtime);

        final SegmentIndexSessionResource<Integer, String> index =
                SegmentIndexSessionFactory.createIndex(resources,
                        effectiveConfiguration("factory-create-index"),
                        keyDescriptor);

        assertInstanceOf(SegmentIndexImpl.class, index);
        final SegmentIndexImpl<Integer, String> implementation =
                castIndex(index);
        assertSame(runtime, implementation.runtime());
        assertSame(infrastructure.stateMachine(),
                implementation.stateMachine());
    }

    private SegmentIndexSessionResources<Integer, String> initializedResources(
            final TypeDescriptorInteger keyDescriptor,
            final SegmentIndexSessionInfrastructure<Integer, String> infrastructure) {
        return initializedResources(infrastructure, newRuntime(keyDescriptor));
    }

    private SegmentIndexSessionResources<Integer, String> initializedResources(
            final SegmentIndexSessionInfrastructure<Integer, String> infrastructure,
            final SegmentIndexRuntime<Integer, String> runtime) {
        final SegmentIndexSessionResources<Integer, String> resources =
                new SegmentIndexSessionResources<>();
        resources.acquireDirectoryLock(new MemDirectory());
        resources.setSessionInfrastructure(infrastructure);
        resources.setRuntime(runtime, mock(ExecutorRegistry.class));
        return resources;
    }

    private EffectiveIndexConfiguration<Integer, String> effectiveConfiguration(
            final String indexName) {
        return effective(configuration(indexName));
    }

    private IndexConfiguration<Integer, String> configuration(
            final String indexName) {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity
                        .keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity
                        .valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name(indexName))
                .logging(logging -> logging.contextEnabled(false))
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
                .filters(filters -> filters.encodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .build();
    }

    @SuppressWarnings("unchecked")
    private SegmentIndexRuntime<Integer, String> newRuntime(
            final TypeDescriptorInteger keyDescriptor) {
        return new SegmentIndexRuntime<>(keyDescriptor,
                mock(StorageService.class), () -> {
                }, mock(SegmentTopologyRuntimeAccess.class),
                new SegmentIndexRuntimeServices<>(
                        mock(SegmentIndexOperationAccess.class),
                        mock(MaintenanceService.class),
                        mock(IndexRuntimeMonitoring.class),
                        mock(RuntimeTuning.class)));
    }

    @SuppressWarnings("unchecked")
    private SegmentIndexImpl<Integer, String> castIndex(
            final SegmentIndexSessionResource<Integer, String> index) {
        return (SegmentIndexImpl<Integer, String>) index;
    }
}
