package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Completes startup recovery and marks the index running.
 */
final class BootstrapStepCompleteStartup<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private static final String STALE_LOCK_RECOVERY_MESSAGE = "Recovered stale index lock (.lock). Index is going to be checked "
            + "for consistency and unlocked.";

    private static final Logger LOGGER = LoggerFactory
            .getLogger(BootstrapStepCompleteStartup.class);

    private final SegmentIndexSessionResources<K, V> sessionResources;

    BootstrapStepCompleteStartup(
            final SegmentIndexSessionResources<K, V> sessionResources) {
        this.sessionResources = Vldtn.requireNonNull(sessionResources,
                "sessionResources");
    }

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        final String indexName = state.getConfiguration().identity().name();

        LOGGER.debug("Opening index '{}'.", indexName);
        sessionResources.recoverFromWal();
        sessionResources.cleanupOrphanedSegmentDirectories();
        sessionResources.markReady();
        if (sessionResources.wasStaleLockRecovered()) {
            LOGGER.info(STALE_LOCK_RECOVERY_MESSAGE);
            sessionResources.runStartupConsistencyCheck();
        }
        sessionResources.requestFullSplitScan();
        LOGGER.debug("Index '{}' opened.", indexName);
    }
}
