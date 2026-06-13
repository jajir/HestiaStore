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
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
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
                        new TypeDescriptorInteger(), newAssemblyInput()));
    }

    @Test
    void createIndex_rejectsNullConfiguration() {
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final SegmentIndexSessionResources<Integer, String> resources =
                initializedResources(SegmentIndexSessionInfrastructure.create());

        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexSessionFactory.createIndex(resources,
                        null, keyDescriptor, newAssemblyInput()));
    }

    @Test
    void createIndex_rejectsNullKeyTypeDescriptor() {
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final SegmentIndexSessionResources<Integer, String> resources =
                initializedResources(SegmentIndexSessionInfrastructure.create());

        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexSessionFactory.createIndex(resources,
                        effectiveConfiguration("factory-null-key"), null,
                        newAssemblyInput()));
    }

    @Test
    void createIndex_rejectsNullAssemblyInput() {
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final SegmentIndexSessionResources<Integer, String> resources =
                initializedResources(SegmentIndexSessionInfrastructure.create());

        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexSessionFactory.createIndex(resources,
                        effectiveConfiguration("factory-null-input"),
                        keyDescriptor, null));
    }

    @Test
    void createIndex_usesInitializedRuntimeAndStateMachine() {
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final SegmentIndexSessionInfrastructure<Integer, String> infrastructure =
                SegmentIndexSessionInfrastructure.create();
        final SegmentIndexSessionResources<Integer, String> resources =
                initializedResources(infrastructure);
        final RuntimeTuning runtimeTuning = mock(RuntimeTuning.class);
        final IndexRuntimeMonitoring runtimeMonitoring =
                mock(IndexRuntimeMonitoring.class);
        final SegmentIndexSessionAssemblyInput<Integer, String> assemblyInput =
                newAssemblyInput(runtimeTuning, runtimeMonitoring);

        final SegmentIndexSessionResource<Integer, String> index =
                SegmentIndexSessionFactory.createIndex(resources,
                        effectiveConfiguration("factory-create-index"),
                        keyDescriptor, assemblyInput);

        assertInstanceOf(SegmentIndexImpl.class, index);
        final SegmentIndexImpl<Integer, String> implementation =
                castIndex(index);
        assertSame(infrastructure.stateMachine(),
                implementation.stateMachine());
        assertSame(runtimeTuning, implementation.runtimeTuning());
        assertSame(runtimeMonitoring, implementation.runtimeMonitoring());
    }

    private SegmentIndexSessionResources<Integer, String> initializedResources(
            final SegmentIndexSessionInfrastructure<Integer, String> infrastructure) {
        final SegmentIndexSessionResources<Integer, String> resources =
                new SegmentIndexSessionResources<>();
        resources.acquireDirectoryLock(new MemDirectory());
        resources.setSessionInfrastructure(infrastructure);
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
    private SegmentIndexSessionAssemblyInput<Integer, String> newAssemblyInput() {
        return newAssemblyInput(mock(RuntimeTuning.class),
                mock(IndexRuntimeMonitoring.class));
    }

    @SuppressWarnings("unchecked")
    private SegmentIndexSessionAssemblyInput<Integer, String> newAssemblyInput(
            final RuntimeTuning runtimeTuning,
            final IndexRuntimeMonitoring runtimeMonitoring) {
        final StorageService<Integer, String> storageService =
                mock(StorageService.class);
        return new SegmentIndexSessionAssemblyInput<>(
                mock(SegmentIndexOperationAccess.class),
                mock(SegmentTopologyRuntimeAccess.class),
                mock(MaintenanceService.class),
                runtimeTuning,
                runtimeMonitoring,
                new CoreStorageRuntime<>(
                        mock(RuntimeTuningState.class),
                        storageService,
                        mock(SegmentRegistry.class),
                        mock(KeyToSegmentMap.class)),
                storageService);
    }

    @SuppressWarnings("unchecked")
    private SegmentIndexImpl<Integer, String> castIndex(
            final SegmentIndexSessionResource<Integer, String> index) {
        return (SegmentIndexImpl<Integer, String>) index;
    }
}
