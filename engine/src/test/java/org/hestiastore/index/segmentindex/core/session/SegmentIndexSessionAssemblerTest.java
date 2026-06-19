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
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.execution.MappedSegmentMaintenanceService;
import org.hestiastore.index.segmentindex.core.execution.PointOperationCoordinator;
import org.hestiastore.index.segmentindex.core.execution.SegmentIteratorService;
import org.hestiastore.index.segmentindex.core.split.SplitRuntime;
import org.hestiastore.index.segmentindex.core.storage.OpenedStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageCoordinator;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.monitoring.SegmentIndexRuntimeMonitoring;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

class SegmentIndexSessionAssemblerTest {

    @Test
    void createIndex_rejectsNullResources() {
        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexSessionAssembler.createIndex(null,
                        effectiveConfiguration("factory-null-resources"),
                        new TypeDescriptorInteger(),
                        mockOperationAccess(),
                        mockSplitService(),
                        mockStreamingService(),
                        mockMaintenanceService(),
                        mock(RuntimeTuning.class),
                        mock(SegmentIndexRuntimeMonitoring.class),
                        newCoreStorageRuntime()));
    }

    @Test
    void createIndex_rejectsNullConfiguration() {
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final SegmentIndexRuntimeResources<Integer, String> resources =
                initializedResources();

        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexSessionAssembler.createIndex(resources,
                        null, keyDescriptor,
                        mockOperationAccess(),
                        mockSplitService(),
                        mockStreamingService(),
                        mockMaintenanceService(),
                        mock(RuntimeTuning.class),
                        mock(SegmentIndexRuntimeMonitoring.class),
                        newCoreStorageRuntime()));
    }

    @Test
    void createIndex_rejectsNullKeyTypeDescriptor() {
        final SegmentIndexRuntimeResources<Integer, String> resources =
                initializedResources();

        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexSessionAssembler.createIndex(resources,
                        effectiveConfiguration("factory-null-key"), null,
                        mockOperationAccess(),
                        mockSplitService(),
                        mockStreamingService(),
                        mockMaintenanceService(),
                        mock(RuntimeTuning.class),
                        mock(SegmentIndexRuntimeMonitoring.class),
                        newCoreStorageRuntime()));
    }

    @Test
    void createIndex_rejectsNullOperationAccess() {
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final SegmentIndexRuntimeResources<Integer, String> resources =
                initializedResources();

        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexSessionAssembler.createIndex(resources,
                        effectiveConfiguration("factory-null-input"),
                        keyDescriptor, null,
                        mockSplitService(),
                        mockStreamingService(),
                        mockMaintenanceService(),
                        mock(RuntimeTuning.class),
                        mock(SegmentIndexRuntimeMonitoring.class),
                        newCoreStorageRuntime()));
    }

    @Test
    void createIndex_usesInitializedRuntimeAndStateMachine() {
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final SegmentIndexRuntimeResources<Integer, String> resources =
                initializedResources();
        final RuntimeTuning runtimeTuning = mock(RuntimeTuning.class);
        final SegmentIndexRuntimeMonitoring runtimeMonitoring = mock(SegmentIndexRuntimeMonitoring.class);

        final SegmentIndex<Integer, String> index = SegmentIndexSessionAssembler.createIndex(resources,
                effectiveConfiguration("factory-create-index"),
                keyDescriptor,
                mockOperationAccess(),
                mockSplitService(),
                mockStreamingService(),
                mockMaintenanceService(),
                runtimeTuning,
                runtimeMonitoring,
                newCoreStorageRuntime());

        assertInstanceOf(SegmentIndexSession.class, index);
        final SegmentIndexSession<Integer, String> implementation = castIndex(index);
        assertSame(resources.stateMachine(),
                implementation.stateMachine());
        assertSame(runtimeTuning, implementation.runtimeTuning());
        assertSame(runtimeMonitoring, implementation.runtimeMonitoring());
    }

    private SegmentIndexRuntimeResources<Integer, String> initializedResources() {
        final SegmentIndexRuntimeResources<Integer, String> resources = new SegmentIndexRuntimeResources<>();
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
    private MappedSegmentMaintenanceService<Integer, String> mockMaintenanceService() {
        return mock(MappedSegmentMaintenanceService.class);
    }

    @SuppressWarnings("unchecked")
    private OpenedStorageRuntime<Integer, String> newCoreStorageRuntime() {
        final StorageCoordinator<Integer, String> storageService = mock(StorageCoordinator.class);
        return new OpenedStorageRuntime<>(
                mock(RuntimeTuningState.class),
                storageService,
                mock(SegmentRegistry.class),
                mock(SegmentRouteMap.class));
    }

    @SuppressWarnings("unchecked")
    private PointOperationCoordinator<Integer, String> mockOperationAccess() {
        return mock(PointOperationCoordinator.class);
    }

    @SuppressWarnings("unchecked")
    private SplitRuntime<Integer, String> mockSplitService() {
        return mock(SplitRuntime.class);
    }

    @SuppressWarnings("unchecked")
    private SegmentIteratorService<Integer, String> mockStreamingService() {
        return mock(SegmentIteratorService.class);
    }

    private SegmentIndexSession<Integer, String> castIndex(
            final SegmentIndex<Integer, String> index) {
        return (SegmentIndexSession<Integer, String>) index;
    }
}
