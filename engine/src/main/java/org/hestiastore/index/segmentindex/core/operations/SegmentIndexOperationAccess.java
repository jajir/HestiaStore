package org.hestiastore.index.segmentindex.core.operations;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentLeaseService;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Capability view exposing only the runtime services needed by data-path and
 * WAL recovery code.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentIndexOperationAccess<K, V> {

    static <K, V> SegmentIndexOperationAccess<K, V> create(
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexOperationStatsRecorder statsRecorder,
            final SegmentLeaseService<K, V> segmentLeaseService,
            final IndexWalCoordinator<K, V> walCoordinator) {
        return new IndexOperationCoordinator<>(
                Vldtn.requireNonNull(valueTypeDescriptor,
                        "valueTypeDescriptor"),
                Vldtn.requireNonNull(statsRecorder, "statsRecorder"),
                Vldtn.requireNonNull(segmentLeaseService,
                        "segmentLeaseService"),
                Vldtn.requireNonNull(walCoordinator, "walCoordinator"));
    }

    void put(K key, V value);

    V get(K key);

    void delete(K key);

    void replayWalRecord(WalRuntime.ReplayRecord<K, V> replayRecord);
}
