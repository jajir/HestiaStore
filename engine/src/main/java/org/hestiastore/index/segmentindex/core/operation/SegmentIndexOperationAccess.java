package org.hestiastore.index.segmentindex.core.operation;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.durability.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.observability.Stats;
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
            final Stats stats,
            final DirectSegmentAccess<K, V> directSegmentCoordinator,
            final IndexWalCoordinator<K, V> walCoordinator,
            final IndexRetryPolicy retryPolicy) {
        return new IndexOperationCoordinator<>(
                Vldtn.requireNonNull(valueTypeDescriptor,
                        "valueTypeDescriptor"),
                Vldtn.requireNonNull(stats, "stats"),
                Vldtn.requireNonNull(directSegmentCoordinator,
                        "directSegmentCoordinator"),
                Vldtn.requireNonNull(walCoordinator, "walCoordinator"),
                Vldtn.requireNonNull(retryPolicy, "retryPolicy"));
    }

    void put(K key, V value);

    V get(K key);

    void delete(K key);

    void replayWalRecord(WalRuntime.ReplayRecord<K, V> replayRecord);
}
