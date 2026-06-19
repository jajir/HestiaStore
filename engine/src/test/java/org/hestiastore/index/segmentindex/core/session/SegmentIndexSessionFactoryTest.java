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
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationCoordinator;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

class SegmentIndexSessionFactoryTest {

    @Test
    void createIndex_rejectsNullResources() {
        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexSessionFactory.createIndex(null,
                        effectiveConfiguration("factory-null-resources"),
                        new TypeDescriptorInteger(),
                        mockOperationAccess(),
                        mockTopologyRuntime(),
                        mockMaintenanceService(),
                        mock(RuntimeTuning.class),
                        mock(IndexRuntimeMonitoring.class),
                        newCoreStorageRuntime()));
    }

    @Test
    void createIndex_rejectsNullConfiguration() {
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final SegmentIndexSessionResources<Integer, String> resources =
                initializedResources();

        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexSessionFactory.createIndex(resources,
                        null, keyDescriptor,
                        mockOperationAccess(),
                        mockTopologyRuntime(),
                        mockMaintenanceService(),
                        mock(RuntimeTuning.class),
                        mock(IndexRuntimeMonitoring.class),
                        newCoreStorageRuntime()));
    }

    @Test
    void createIndex_rejectsNullKeyTypeDescriptor() {
        final SegmentIndexSessionResources<Integer, String> resources =
                initializedResources();

        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexSessionFactory.createIndex(resources,
                        effectiveConfiguration("factory-null-key"), null,
                        mockOperationAccess(),
                        mockTopologyRuntime(),
                        mockMaintenanceService(),
                        mock(RuntimeTuning.class),
                        mock(IndexRuntimeMonitoring.class),
                        newCoreStorageRuntime()));
    }

    @Test
    void createIndex_rejectsNullOperationAccess() {
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final SegmentIndexSessionResources<Integer, String> resources =
                initializedResources();

        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexSessionFactory.createIndex(resources,
                        effectiveConfiguration("factory-null-input"),
                        keyDescriptor, null,
                        mockTopologyRuntime(),
                        mockMaintenanceService(),
                        mock(RuntimeTuning.class),
                        mock(IndexRuntimeMonitoring.class),
                        newCoreStorageRuntime()));
    }

    @Test
    void createIndex_usesInitializedRuntimeAndStateMachine() {
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final SegmentIndexSessionResources<Integer, String> resources =
                initializedResources();
        final RuntimeTuning runtimeTuning = mock(RuntimeTuning.class);
        final IndexRuntimeMonitoring runtimeMonitoring = mock(IndexRuntimeMonitoring.class);

        final SegmentIndex<Integer, String> index = SegmentIndexSessionFactory.createIndex(resources,
                effectiveConfiguration("factory-create-index"),
                keyDescriptor,
                mockOperationAccess(),
                mockTopologyRuntime(),
                mockMaintenanceService(),
                runtimeTuning,
                runtimeMonitoring,
                newCoreStorageRuntime());

        assertInstanceOf(SegmentIndexImpl.class, index);
        final SegmentIndexImpl<Integer, String> implementation = castIndex(index);
        assertSame(resources.stateMachine(),
                implementation.stateMachine());
        assertSame(runtimeTuning, implementation.runtimeTuning());
        assertSame(runtimeMonitoring, implementation.runtimeMonitoring());
    }

    private SegmentIndexSessionResources<Integer, String> initializedResources() {
        final SegmentIndexSessionResources<Integer, String> resources = new SegmentIndexSessionResources<>();
        resources.acquireDirectoryLock(new MemDirectory());
        resources.setExecutorRegistry(mock(ExecutorRegistry.class));
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
    private MaintenanceService<Integer, String> mockMaintenanceService() {
        return mock(MaintenanceService.class);
    }

    @SuppressWarnings("unchecked")
    private CoreStorageRuntime<Integer, String> newCoreStorageRuntime() {
        final StorageService<Integer, String> storageService = mock(StorageService.class);
        return new CoreStorageRuntime<>(
                mock(RuntimeTuningState.class),
                storageService,
                mock(SegmentRegistry.class),
                mock(KeyToSegmentMap.class));
    }

    @SuppressWarnings("unchecked")
    private IndexOperationCoordinator<Integer, String> mockOperationAccess() {
        return mock(IndexOperationCoordinator.class);
    }

    @SuppressWarnings("unchecked")
    private SegmentTopologyRuntimeAccess<Integer, String> mockTopologyRuntime() {
        return mock(SegmentTopologyRuntimeAccess.class);
    }

    private SegmentIndexImpl<Integer, String> castIndex(
            final SegmentIndex<Integer, String> index) {
        return (SegmentIndexImpl<Integer, String>) index;
    }
}
