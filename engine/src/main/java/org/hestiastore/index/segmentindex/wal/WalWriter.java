package org.hestiastore.index.segmentindex.wal;

import java.util.Objects;

/**
 * Serial WAL record writer. Callers serialize access and handle durability.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class WalWriter<K, V> {

    private final WalStorage storage;
    private final WalRecordCodec<K, V> recordCodec;
    private final WalSegmentCatalog segmentCatalog;
    private final WalRuntimeMetrics metrics;
    private long nextLsn = 1L;

    WalWriter(final WalStorage storage,
            final WalRecordCodec<K, V> recordCodec,
            final WalSegmentCatalog segmentCatalog,
            final WalRuntimeMetrics metrics) {
        this.storage = storage;
        this.recordCodec = recordCodec;
        this.segmentCatalog = segmentCatalog;
        this.metrics = metrics;
    }

    void resetNextLsn(final long nextLsn) {
        this.nextLsn = nextLsn;
    }

    /**
     * Appends one encoded WAL record and returns durability metadata.
     *
     * @param operation operation kind
     * @param key       record key
     * @param value     record value
     * @return append result
     */
    WalAppendResult append(final WalRuntime.Operation operation, final K key,
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
        return new WalAppendResult(lsn, recordBytes.length,
                activeSegment.name());
    }
}
