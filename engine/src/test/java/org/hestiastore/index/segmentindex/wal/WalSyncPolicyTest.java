package org.hestiastore.index.segmentindex.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.WalDurabilityMode;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class WalSyncPolicyTest {

    @Test
    void syncModeFlushesPendingBytesImmediately() {
        final Wal wal = Wal.builder().withDurabilityMode(WalDurabilityMode.SYNC)
                .build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        final WalMetadataCatalog metadataCatalog = new WalMetadataCatalog(
                storage, LoggerFactory.getLogger(WalSyncPolicyTest.class));
        final WalSegmentCatalog segmentCatalog = new WalSegmentCatalog(wal,
                storage, metadataCatalog,
                LoggerFactory.getLogger(WalSyncPolicyTest.class));
        final WalRuntimeMetrics metrics = new WalRuntimeMetrics();
        final Object monitor = new Object();
        final AtomicBoolean closed = new AtomicBoolean(false);
        final WalSyncPolicy syncPolicy = new WalSyncPolicy(wal, storage, metrics,
                LoggerFactory.getLogger(WalSyncPolicyTest.class), monitor,
                segmentCatalog::segments, closed::get);

        synchronized (monitor) {
            final WalSegmentDescriptor segment = segmentCatalog
                    .ensureActiveSegmentFor(1L, 16);
            syncPolicy.afterAppend(1L, 16, segment.name());
        }

        assertEquals(1L, syncPolicy.durableLsn());
        assertEquals(0L, syncPolicy.pendingSyncBytes());
    }
}
