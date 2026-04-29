package org.hestiastore.index.segmentindex.wal;

import java.util.Objects;

import org.hestiastore.index.segmentindex.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.WalDurabilityMode;

final class WalWriter<K, V> {

    private final IndexWalConfiguration wal;
    private final WalStorage storage;
    private final WalRecordCodec<K, V> recordCodec;
    private final WalSegmentCatalog segmentCatalog;
    private final WalRuntimeMetrics metrics;
    private final WalSyncPolicy syncPolicy;
    private long nextLsn = 1L;

    WalWriter(final IndexWalConfiguration wal, final WalStorage storage,
            final WalRecordCodec<K, V> recordCodec,
            final WalSegmentCatalog segmentCatalog,
            final WalRuntimeMetrics metrics, final WalSyncPolicy syncPolicy) {
        this.wal = wal;
        this.storage = storage;
        this.recordCodec = recordCodec;
        this.segmentCatalog = segmentCatalog;
        this.metrics = metrics;
        this.syncPolicy = syncPolicy;
    }

    void resetNextLsn(final long nextLsn) {
        this.nextLsn = nextLsn;
    }

    long append(final WalRuntime.Operation operation, final K key,
            final V value) {
        Objects.requireNonNull(key, "key");
        if (operation == WalRuntime.Operation.PUT) {
            Objects.requireNonNull(value, "value");
        }
        final long lsn = nextLsn;
        final byte[] recordBytes = recordCodec.encodeRecord(operation, lsn, key,
                value);
        final WalSegmentDescriptor activeSegment = segmentCatalog
                .ensureActiveSegmentFor(nextLsn, recordBytes.length);
        storage.append(activeSegment.name(), recordBytes, 0, recordBytes.length);
        segmentCatalog.recordAppend(activeSegment, recordBytes.length, lsn);
        metrics.recordAppend(recordBytes.length);
        nextLsn = lsn + 1L;
        if (wal.getDurabilityMode() == WalDurabilityMode.ASYNC) {
            return lsn;
        }
        syncPolicy.afterAppend(lsn, recordBytes.length, activeSegment.name());
        return lsn;
    }
}
