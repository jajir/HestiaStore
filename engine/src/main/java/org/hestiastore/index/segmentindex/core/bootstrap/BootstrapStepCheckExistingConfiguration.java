package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStorage;

/**
 * Rejects impossible open/create modes before locking the directory.
 */
final class BootstrapStepCheckExistingConfiguration<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private static final String ERROR_INDEX_ALREADY_EXISTS =
            "Cannot create segment index because configuration already exists.";
    private static final String ERROR_INDEX_NOT_FOUND =
            "Cannot open segment index because configuration does not exist.";

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {

        final boolean configurationExists =
                new IndexConfigurationStorage<K, V>(request.getDirectory())
                        .exists();

        if (configurationExists) {
            if (request.getMode().isCreate()) {
                throw new IndexException(ERROR_INDEX_ALREADY_EXISTS);
            }
        } else {
            if (request.getMode().isTryOpen()) {
                state.setResult(SegmentIndexBootstrapResult.notFound());
            } else if (request.getMode() == SegmentIndexBootstrapMode.OPEN) {
                throw new IndexException(ERROR_INDEX_NOT_FOUND);
            }
        }
    }
}
