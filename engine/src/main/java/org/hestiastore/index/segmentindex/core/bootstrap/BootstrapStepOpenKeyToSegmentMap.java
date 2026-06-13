package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;

/**
 * Opens the persisted key-to-segment route map.
 */
final class BootstrapStepOpenKeyToSegmentMap<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private SegmentIndexBootstrapState<K, V> state;
    private KeyToSegmentMap<K> keyToSegmentMap;

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        this.state = state;
        final KeyToSegmentMapImpl<K> keyToSegmentMapDelegate =
                new KeyToSegmentMapImpl<>(request.getDirectory(),
                        state.getKeyTypeDescriptor());
        keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMapDelegate);
        state.setKeyToSegmentMap(keyToSegmentMap);
    }

    @Override
    void closeResource() {
        if (state == null || state.runtimeCloseOwnershipTransferred()
                || keyToSegmentMap == null || keyToSegmentMap.wasClosed()) {
            return;
        }
        keyToSegmentMap.close();
    }
}
