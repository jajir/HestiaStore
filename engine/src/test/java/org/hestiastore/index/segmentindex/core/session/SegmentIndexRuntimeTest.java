package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexRuntimeTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    private ExecutorRegistry executorRegistry;
    private Object openedRuntime;
    private SegmentIndexRuntime<Integer, String> runtime;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        executorRegistry = ExecutorRegistryFixture.from(conf);
        openedRuntime = SegmentIndexRuntimeTestAccess.openRuntime(
                new MemDirectory(), tdi, tds, conf, executorRegistry);
        runtime = SegmentIndexRuntimeTestAccess.runtime(openedRuntime);
    }

    @AfterEach
    void tearDown() {
        if (openedRuntime != null) {
            SegmentIndexRuntimeTestAccess.closeRuntime(openedRuntime,
                    "runtime-test", executorRegistry);
        }
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }

    @Test
    void openBuildsAllCoreCollaborators() {
        assertNotNull(SegmentIndexRuntimeTestAccess.keyToSegmentMap(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.segmentRegistry(runtime));
        assertNotNull(
                SegmentIndexRuntimeTestAccess.runtimeTuningState(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.storageService(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.operationAccess(runtime));
        assertNotNull(SegmentIndexRuntimeTestAccess.runtimeTuning(runtime));
    }

    @Test
    @SuppressWarnings("unchecked")
    void closeAfterFailedInitializationClosesRuntimeResourcesInOrder() {
        final SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime =
                mock(SegmentTopologyRuntimeAccess.class);
        final KeyToSegmentMap<Integer> keyToSegmentMap =
                mock(KeyToSegmentMap.class);
        final SegmentRegistry<Integer, String> segmentRegistry =
                mock(SegmentRegistry.class);
        final StorageService<Integer, String> storageService =
                mock(StorageService.class);
        when(keyToSegmentMap.wasClosed()).thenReturn(false);
        final SegmentIndexRuntime<Integer, String> failedRuntime =
                newRuntime(topologyRuntime, keyToSegmentMap, segmentRegistry,
                        storageService);

        failedRuntime.closeAfterFailedInitialization();

        final InOrder inOrder = inOrder(topologyRuntime, segmentRegistry,
                keyToSegmentMap, storageService);
        inOrder.verify(topologyRuntime).closeSplitRuntime();
        inOrder.verify(segmentRegistry).close();
        inOrder.verify(keyToSegmentMap).wasClosed();
        inOrder.verify(keyToSegmentMap).close();
        inOrder.verify(storageService).closeWal();
    }

    @Test
    @SuppressWarnings("unchecked")
    void recoveryCleanupOperationsDelegateToStorageService() {
        final SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime =
                mock(SegmentTopologyRuntimeAccess.class);
        final KeyToSegmentMap<Integer> keyToSegmentMap =
                mock(KeyToSegmentMap.class);
        final SegmentRegistry<Integer, String> segmentRegistry =
                mock(SegmentRegistry.class);
        final StorageService<Integer, String> storageService =
                mock(StorageService.class);
        final SegmentId segmentId = SegmentId.of(3);
        when(storageService.hasSegmentLockFile(segmentId)).thenReturn(true);
        final SegmentIndexRuntime<Integer, String> runtimeWithStorageService =
                newRuntime(topologyRuntime, keyToSegmentMap, segmentRegistry,
                        storageService);

        runtimeWithStorageService.cleanupOrphanedSegmentDirectories();
        final boolean lockFileExists =
                runtimeWithStorageService.hasSegmentLockFile(segmentId);

        verify(storageService).cleanupOrphanedSegmentDirectories();
        verify(storageService).hasSegmentLockFile(segmentId);
        assertTrue(lockFileExists);
    }

    @Test
    @SuppressWarnings("unchecked")
    void consistencyOperationsDelegateToStorageServiceAndRequestSplitScan() {
        final SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime =
                mock(SegmentTopologyRuntimeAccess.class);
        final KeyToSegmentMap<Integer> keyToSegmentMap =
                mock(KeyToSegmentMap.class);
        final SegmentRegistry<Integer, String> segmentRegistry =
                mock(SegmentRegistry.class);
        final StorageService<Integer, String> storageService =
                mock(StorageService.class);
        final SegmentIndexRuntime<Integer, String> runtimeWithStorageService =
                newRuntime(topologyRuntime, keyToSegmentMap, segmentRegistry,
                        storageService);

        runtimeWithStorageService.checkAndRepairConsistency();
        runtimeWithStorageService.runStartupConsistencyCheck();

        final InOrder inOrder = inOrder(storageService, topologyRuntime);
        inOrder.verify(storageService).checkAndRepairConsistency();
        inOrder.verify(topologyRuntime).requestFullSplitScan();
        inOrder.verify(storageService).runStartupConsistencyCheck();
        inOrder.verify(topologyRuntime).requestFullSplitScan();
    }

    @SuppressWarnings("unchecked")
    private SegmentIndexRuntime<Integer, String> newRuntime(
            final SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime,
            final KeyToSegmentMap<Integer> keyToSegmentMap,
            final SegmentRegistry<Integer, String> segmentRegistry,
            final StorageService<Integer, String> storageService) {
        return newRuntime(topologyRuntime, keyToSegmentMap, segmentRegistry,
                storageService, mock(SegmentIndexRuntimeServices.class));
    }

    @SuppressWarnings("unchecked")
    private SegmentIndexRuntime<Integer, String> newRuntime(
            final SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime,
            final KeyToSegmentMap<Integer> keyToSegmentMap,
            final SegmentRegistry<Integer, String> segmentRegistry,
            final StorageService<Integer, String> storageService,
            final SegmentIndexRuntimeServices<Integer, String> services) {
        return new SegmentIndexRuntime<>(tdi,
                new SegmentIndexCoreStorage<>(
                        mock(RuntimeTuningState.class), keyToSegmentMap,
                        segmentRegistry, mock(ChunkStoreCache.class),
                        mock(IndexRetryPolicy.class), storageService),
                topologyRuntime, services);
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(tdi))
                .identity(identity -> identity.valueTypeDescriptor(tds))
                .identity(identity -> identity.name("segment-index-runtime-test"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }

}
