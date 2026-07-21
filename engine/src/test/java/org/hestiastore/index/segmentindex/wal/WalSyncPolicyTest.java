package org.hestiastore.index.segmentindex.wal;

import static org.hestiastore.index.segmentindex.wal.WalRuntimeTestSupport.effective;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.configuration.api.WalDurabilityMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WalSyncPolicyTest {

    @Mock
    private WalStorage storage;

    @Test
    void syncModeFlushesPendingBytesAtAppendBatchBoundary() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder()
                .durability(WalDurabilityMode.SYNC)
                .build();
        final WalStorageMem memStorage = new WalStorageMem(new MemDirectory());
        final WalMetadataCatalog metadataCatalog = new WalMetadataCatalog(
                memStorage);
        final WalSegmentCatalog segmentCatalog = new WalSegmentCatalog(
                effective(wal), memStorage, metadataCatalog);
        final WalRuntimeMetrics metrics = new WalRuntimeMetrics();
        final Object monitor = new Object();
        final WalSyncPolicy syncPolicy = new WalSyncPolicy(effective(wal),
                memStorage, metrics, monitor, segmentCatalog);

        synchronized (monitor) {
            final WalSegmentDescriptor segment = segmentCatalog
                    .ensureActiveSegmentFor(1L, 16);
            syncPolicy.afterAppend(1L, 16, segment.name());
            syncPolicy.afterAppend(2L, 16, segment.name());
            assertEquals(0L, syncPolicy.durableLsn());
            assertEquals(32L, syncPolicy.pendingSyncBytes());
            syncPolicy.afterAppendBatch();
        }

        final WalMonitoring snapshot = metrics.snapshot(0L, 1,
                syncPolicy.durableLsn(), 0L,
                syncPolicy.pendingSyncBytes());
        assertEquals(2L, syncPolicy.durableLsn());
        assertEquals(0L, syncPolicy.pendingSyncBytes());
        assertEquals(1L, snapshot.syncCount());
        assertEquals(32L, snapshot.syncBatchBytesTotal());
    }

    @Test
    void syncModeSyncsSegmentMetadataOnlyAfterSegmentCreation() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder()
                .durability(WalDurabilityMode.SYNC)
                .build();
        final WalMetadataCatalog metadataCatalog = new WalMetadataCatalog(
                storage);
        final WalSegmentCatalog segmentCatalog = new WalSegmentCatalog(
                effective(wal), storage, metadataCatalog);
        final WalRuntimeMetrics metrics = new WalRuntimeMetrics();
        final Object monitor = new Object();
        final WalSyncPolicy syncPolicy = new WalSyncPolicy(effective(wal),
                storage, metrics, monitor, segmentCatalog);
        final WalSegmentDescriptor segment;

        synchronized (monitor) {
            segment = segmentCatalog.ensureActiveSegmentFor(1L, 16);
            syncPolicy.afterAppend(1L, 16, segment.name());
            syncPolicy.afterAppend(2L, 16, segment.name());
            syncPolicy.afterAppendBatch();
        }

        verify(storage).sync(segment.name());
        verify(storage).syncMetadata();
    }

    @Test
    void groupSyncFlushesExpiredDeadlineAtAppendBatchBoundary() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder()
                .durability(WalDurabilityMode.GROUP_SYNC)
                .groupSyncDelayMillis(1)
                .groupSyncMaxBatchBytes(1024 * 1024)
                .build();
        final WalStorageMem memStorage = new WalStorageMem(new MemDirectory());
        final WalMetadataCatalog metadataCatalog = new WalMetadataCatalog(
                memStorage);
        final WalSegmentCatalog segmentCatalog = new WalSegmentCatalog(
                effective(wal), memStorage, metadataCatalog);
        final WalRuntimeMetrics metrics = new WalRuntimeMetrics();
        final Object monitor = new Object();
        final WalSyncPolicy syncPolicy = new WalSyncPolicy(effective(wal),
                memStorage, metrics, monitor, segmentCatalog);

        synchronized (monitor) {
            final WalSegmentDescriptor segment = segmentCatalog
                    .ensureActiveSegmentFor(1L, 16);
            syncPolicy.afterAppend(1L, 16, segment.name());
            assertTrue(syncPolicy.pendingGroupSyncWaitNanosLocked() > 0L);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
            syncPolicy.afterAppendBatch();
        }

        assertEquals(1L, syncPolicy.durableLsn());
        assertEquals(0L, syncPolicy.pendingSyncBytes());
    }
}
