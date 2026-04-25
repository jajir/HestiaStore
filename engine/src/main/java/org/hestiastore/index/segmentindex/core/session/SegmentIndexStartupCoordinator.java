package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.storage.IndexConsistencyCoordinator;
import org.hestiastore.index.segmentindex.core.session.state.IndexStateCoordinator;
import org.slf4j.Logger;

/**
 * Owns the one-shot startup sequence that recovers runtime state and marks the
 * index ready.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexStartupCoordinator<K, V> {

    private static final String STALE_LOCK_RECOVERY_MESSAGE = "Recovered stale index lock (.lock). Index is going to be checked for consistency and unlocked.";

    private final Logger logger;
    private final String indexName;
    private final boolean staleLockRecovered;
    private final SegmentIndexRuntime<K, V> runtime;
    private final IndexStateCoordinator<K, V> stateCoordinator;
    private final IndexConsistencyCoordinator<K, V> consistencyCoordinator;

    SegmentIndexStartupCoordinator(final Logger logger,
            final String indexName, final boolean staleLockRecovered,
            final SegmentIndexRuntime<K, V> runtime,
            final IndexStateCoordinator<K, V> stateCoordinator,
            final IndexConsistencyCoordinator<K, V> consistencyCoordinator) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.indexName = Vldtn.requireNonNull(indexName, "indexName");
        this.staleLockRecovered = staleLockRecovered;
        this.runtime = Vldtn.requireNonNull(runtime, "runtime");
        this.stateCoordinator = Vldtn.requireNonNull(stateCoordinator,
                "stateCoordinator");
        this.consistencyCoordinator = Vldtn.requireNonNull(
                consistencyCoordinator, "consistencyCoordinator");
    }

    void completeStartup(final Runnable startupConsistencyCheck) {
        final Runnable consistencyCheck = Vldtn.requireNonNull(
                startupConsistencyCheck, "startupConsistencyCheck");
        logger.debug("Opening index '{}'.", indexName);
        runtime.recoverFromWal();
        runtime.cleanupOrphanedSegmentDirectories();
        stateCoordinator.markReady();
        if (staleLockRecovered) {
            logger.info(STALE_LOCK_RECOVERY_MESSAGE);
            consistencyCoordinator.runStartupConsistencyCheck(consistencyCheck);
        }
        runtime.requestSplitReconciliation();
        logger.debug("Index '{}' opened.", indexName);
    }
}
