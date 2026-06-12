package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionInfrastructure;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;

/**
 * Creates the state machine, stats, operation tracker, and tracked runner.
 */
final class BootstrapStepCreateSessionInfrastructure<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private final SegmentIndexSessionResources<K, V> sessionResources;

    BootstrapStepCreateSessionInfrastructure(
            final SegmentIndexSessionResources<K, V> sessionResources) {
        this.sessionResources = Vldtn.requireNonNull(sessionResources,
                "sessionResources");
    }

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        sessionResources.setSessionInfrastructure(
                SegmentIndexSessionInfrastructure.create());
    }
}
