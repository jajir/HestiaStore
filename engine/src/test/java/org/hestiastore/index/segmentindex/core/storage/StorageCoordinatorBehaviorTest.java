package org.hestiastore.index.segmentindex.core.storage;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.SegmentIndexRuntimeState;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.PersistentSegmentRouteMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StorageCoordinatorBehaviorTest {

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private WalRuntime<Integer, String> walRuntime;

    private Directory directory;
    private PersistentSegmentRouteMap<Integer> keyToSegmentMap;
    private SegmentRouteMap<Integer> synchronizedKeyToSegmentMap;
    private StorageCoordinator<Integer, String> storageService;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        keyToSegmentMap = new PersistentSegmentRouteMap<>(directory,
                new TypeDescriptorInteger());
        synchronizedKeyToSegmentMap = keyToSegmentMap;
        storageService = StorageCoordinator.create(directory,
                synchronizedKeyToSegmentMap, segmentRegistry,
                effective(buildConf()).maintenance());
    }

    @AfterEach
    void tearDown() {
        if (synchronizedKeyToSegmentMap != null
                && !synchronizedKeyToSegmentMap.wasClosed()) {
            synchronizedKeyToSegmentMap.close();
        }
    }

    @Test
    void cleanupOrphanedSegmentDirectories_deletesOnlyUnmappedNonBootstrapDirectories() {
        synchronizedKeyToSegmentMap.extendMaxKeyIfNeeded(1);
        directory.mkdir("segment-00000");
        directory.mkdir("segment-00003");
        when(segmentRegistry.deleteSegmentIfAvailable(SegmentId.of(3)))
                .thenReturn(true);

        storageService.cleanupOrphanedSegmentDirectories();

        verify(segmentRegistry).deleteSegmentIfAvailable(SegmentId.of(3));
        verify(segmentRegistry, never())
                .deleteSegmentIfAvailable(SegmentId.of(0));
        verify(segmentRegistry, never())
                .deleteSegmentIfAvailable(SegmentId.of(1));
    }

    @Test
    void cleanupOrphanedSegmentDirectories_removesChildrenLeftByUnflushedSplit() {
        synchronizedKeyToSegmentMap.extendMaxKeyIfNeeded(100);
        synchronizedKeyToSegmentMap.flushIfDirty();
        directory.mkdir("segment-00000");
        directory.mkdir("segment-00001");
        directory.mkdir("segment-00002");
        when(segmentRegistry.deleteSegmentIfAvailable(SegmentId.of(1)))
                .thenReturn(true);
        when(segmentRegistry.deleteSegmentIfAvailable(SegmentId.of(2)))
                .thenReturn(true);

        storageService.cleanupOrphanedSegmentDirectories();

        verify(segmentRegistry, never())
                .deleteSegmentIfAvailable(SegmentId.of(0));
        verify(segmentRegistry).deleteSegmentIfAvailable(SegmentId.of(1));
        verify(segmentRegistry).deleteSegmentIfAvailable(SegmentId.of(2));
    }

    @Test
    void walOperationsAreDisabledBeforeInitialization() {
        assertEquals(0L, storageService.appendWalPut(1, "one"));
        assertEquals(0L, storageService.appendWalDelete(1));

        storageService.recoverFromWal(replayRecord -> {
        });
        storageService.checkpointWal();
        storageService.recordAppliedWalLsn(7L);
        storageService.closeWal();

        verify(walRuntime, never()).appendPut(1, "one");
    }

    @Test
    void initializeWal_enablesWalOperationsThroughService() {
        final AtomicLong lastAppliedWalLsn = new AtomicLong(0L);
        when(walRuntime.appendPut(1, "one")).thenReturn(7L);

        storageService.initializeWal(effective(buildConf()), walRuntime,
                mock(WalCheckpointMaintenance.class), runtimeState(),
                lastAppliedWalLsn);
        final long walLsn = storageService.appendWalPut(1, "one");
        storageService.recordAppliedWalLsn(walLsn);
        storageService.checkpointWal();
        storageService.closeWal();

        assertEquals(7L, walLsn);
        assertEquals(7L, lastAppliedWalLsn.get());
        verify(walRuntime).appendPut(1, "one");
        verify(walRuntime).onCheckpoint(7L);
        verify(walRuntime).close();
    }

    private SegmentIndexRuntimeState runtimeState() {
        return new SegmentIndexRuntimeState() {

            @Override
            public SegmentIndexState currentState() {
                return SegmentIndexState.READY;
            }

            @Override
            public void markRuntimeFailure(final RuntimeException failure) {
            }
        };
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("storage-service-wal-test"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(7))
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(9))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(50))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .wal(wal -> wal.configuration(IndexWalConfiguration.builder()
                        .maxBytesBeforeForcedCheckpoint(1024L)
                        .build()))
                .build();
    }
}
