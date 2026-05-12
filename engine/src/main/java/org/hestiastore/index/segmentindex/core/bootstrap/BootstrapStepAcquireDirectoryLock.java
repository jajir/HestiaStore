package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;

/**
 * Acquires the index directory lock.
 */
final class BootstrapStepAcquireDirectoryLock<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private final SegmentIndexSessionResources<K, V> sessionResources;

    BootstrapStepAcquireDirectoryLock(
            final SegmentIndexSessionResources<K, V> sessionResources) {
        this.sessionResources = Vldtn.requireNonNull(sessionResources,
                "sessionResources");
    }

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        sessionResources.acquireDirectoryLock(request.getDirectory());
    }

    @Override
    void closeResource() {
        // Intentionally keep the index directory locked after failed
        // initialization.
    }
}
