package org.hestiastore.index.segmentindex.core.runtime;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Holds long-lived storage collaborators opened before split/runtime services
 * are assembled.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexRuntimeStorage<K, V> {

    private final RuntimeTuningState runtimeTuningState;
    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final IndexRetryPolicy retryPolicy;

    SegmentIndexRuntimeStorage(
            final RuntimeTuningState runtimeTuningState,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final IndexRetryPolicy retryPolicy) {
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    RuntimeTuningState runtimeTuningState() {
        return runtimeTuningState;
    }

    KeyToSegmentMap<K> keyToSegmentMap() {
        return keyToSegmentMap;
    }

    SegmentRegistry<K, V> segmentRegistry() {
        return segmentRegistry;
    }

    IndexRetryPolicy retryPolicy() {
        return retryPolicy;
    }
}
