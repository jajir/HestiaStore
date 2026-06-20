package org.hestiastore.index.segmentindex.wal;

import static org.hestiastore.index.segmentindex.wal.WalRuntimeTestSupport.effective;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.configuration.api.WalDurabilityMode;
import org.junit.jupiter.api.Test;

class WalSyncPolicyTest {

    @Test
    void syncModeFlushesPendingBytesImmediately() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder()
                .durability(WalDurabilityMode.SYNC)
                .build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        final WalMetadataCatalog metadataCatalog = new WalMetadataCatalog(
                storage);
        final WalSegmentCatalog segmentCatalog = new WalSegmentCatalog(
                effective(wal), storage, metadataCatalog);
        final WalRuntimeMetrics metrics = new WalRuntimeMetrics();
        final Object monitor = new Object();
        final AtomicBoolean closed = new AtomicBoolean(false);
        final WalSyncPolicy syncPolicy = new WalSyncPolicy(effective(wal),
                storage, metrics, monitor, segmentCatalog, closed);

        synchronized (monitor) {
            final WalSegmentDescriptor segment = segmentCatalog
                    .ensureActiveSegmentFor(1L, 16);
            syncPolicy.afterAppend(1L, 16, segment.name());
        }

        assertEquals(1L, syncPolicy.durableLsn());
        assertEquals(0L, syncPolicy.pendingSyncBytes());
    }
}
