package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStorage;

/**
 * Rejects create mode when a persisted index configuration already exists.
 */
final class BootstrapStepRejectExistingConfigurationForCreate<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        if (request.getMode() != SegmentIndexBootstrapMode.CREATE) {
            return;
        }
        if (new IndexConfigurationStorage<K, V>(request.getDirectory())
                .exists()) {
            throw new IndexException(
                    "Cannot create segment index because configuration already exists.");
        }
    }
}
