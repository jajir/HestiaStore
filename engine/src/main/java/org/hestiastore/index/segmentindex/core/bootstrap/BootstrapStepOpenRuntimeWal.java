package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexWalConfiguration;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Opens WAL runtime resources and owns rollback cleanup until runtime creation.
 */
final class BootstrapStepOpenRuntimeWal<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private SegmentIndexBootstrapState<K, V> state;
    private WalRuntime<K, V> walRuntime;

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        this.state = state;
        final EffectiveIndexWalConfiguration wal = state.getConfiguration().wal();
        if (!wal.isEnabled()) {
            return;
        }
        walRuntime = WalRuntime.open(request.getDirectory(), wal,
                state.getKeyTypeDescriptor(),
                state.getValueTypeDescriptor());
        state.setRuntimeWalRuntime(walRuntime);
    }

    @Override
    void closeResource() {
        if (state == null || state.indexRuntimeWasCreated()
                || walRuntime == null) {
            return;
        }
        walRuntime.close();
    }

}
