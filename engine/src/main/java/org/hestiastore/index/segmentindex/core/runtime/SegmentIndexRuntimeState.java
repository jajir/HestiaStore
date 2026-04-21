package org.hestiastore.index.segmentindex.core.runtime;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Holds the long-lived collaborators created for one running segment index.
 * Operations and lifecycle protocols sit on top of this state holder.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexRuntimeState<K, V> {

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final SegmentIndexRuntimeStorage<K, V> storage;
    private final SegmentIndexRuntimeSplits<K, V> splits;
    private final SegmentIndexRuntimeServices<K, V> services;
    private final WalRuntime<K, V> walRuntime;

    private SegmentIndexRuntimeState(
            final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentIndexRuntimeStorage<K, V> storage,
            final SegmentIndexRuntimeSplits<K, V> splits,
            final SegmentIndexRuntimeServices<K, V> services,
            final WalRuntime<K, V> walRuntime) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.storage = Vldtn.requireNonNull(storage, "storage");
        this.splits = Vldtn.requireNonNull(splits, "splits");
        this.services = Vldtn.requireNonNull(services, "services");
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
    }

    static <K, V> SegmentIndexRuntimeState<K, V> create(
            final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentIndexRuntimeSplits<K, V> splitState,
            final WalRuntime<K, V> walRuntime,
            final SegmentIndexRuntimeServices<K, V> serviceState) {
        final SegmentIndexCoreStorage<K, V> validatedCoreStorage = Vldtn
                .requireNonNull(coreStorage, "coreStorage");
        final SegmentIndexRuntimeSplits<K, V> validatedSplitState = Vldtn
                .requireNonNull(splitState, "splitState");
        final SegmentIndexRuntimeServices<K, V> validatedServiceState = Vldtn
                .requireNonNull(serviceState, "serviceState");
        return new SegmentIndexRuntimeState<>(
                Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor"),
                new SegmentIndexRuntimeStorage<>(
                        validatedCoreStorage.runtimeTuningState(),
                        validatedCoreStorage.keyToSegmentMap(),
                        validatedCoreStorage.segmentRegistry(),
                        validatedCoreStorage.retryPolicy()),
                validatedSplitState,
                validatedServiceState,
                Vldtn.requireNonNull(walRuntime, "walRuntime"));
    }

    TypeDescriptor<K> keyTypeDescriptor() {
        return keyTypeDescriptor;
    }

    WalRuntime<K, V> walRuntime() {
        return walRuntime;
    }

    SegmentIndexRuntimeStorage<K, V> storage() {
        return storage;
    }

    SegmentIndexRuntimeSplits<K, V> splits() {
        return splits;
    }

    SegmentIndexRuntimeServices<K, V> services() {
        return services;
    }
}
