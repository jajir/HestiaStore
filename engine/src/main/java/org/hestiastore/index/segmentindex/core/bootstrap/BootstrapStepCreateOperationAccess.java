package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;

/**
 * Creates point-operation and WAL replay access after storage coordination is
 * initialized.
 */
final class BootstrapStepCreateOperationAccess<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private final SegmentIndexSessionResources<K, V> sessionResources;

    BootstrapStepCreateOperationAccess(
            final SegmentIndexSessionResources<K, V> sessionResources) {
        this.sessionResources = Vldtn.requireNonNull(sessionResources,
                "sessionResources");
    }

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        state.setRuntimeOperationAccess(SegmentIndexOperationAccess.create(
                state.getValueTypeDescriptor(),
                sessionResources.operationStatsRecorder(),
                state.getRuntimeSegmentLeaseService(),
                state.getStorageService()));
    }
}
