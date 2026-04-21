package org.hestiastore.index.segmentindex.core.lifecycle;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.consistency.IndexConsistencyCoordinator;
import org.hestiastore.index.segmentindex.core.state.IndexStateCoordinator;
import org.slf4j.Logger;

/**
 * Owns the one-shot startup sequence that recovers runtime state and marks the
 * index ready.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexStartupCoordinator<K, V> {

    private static final String STALE_LOCK_RECOVERY_MESSAGE = "Recovered stale index lock (.lock). Index is going to be checked for consistency and unlocked.";

    private final Logger logger;
    private final String indexName;
    private final boolean staleLockRecovered;
    private final SegmentIndexStartupAccess<K, V> startupAccess;
    private final IndexStateCoordinator<K, V> stateCoordinator;
    private final IndexConsistencyCoordinator<K, V> consistencyCoordinator;

    public SegmentIndexStartupCoordinator(final Logger logger,
            final String indexName, final boolean staleLockRecovered,
            final SegmentIndexStartupAccess<K, V> startupAccess,
            final IndexStateCoordinator<K, V> stateCoordinator,
            final IndexConsistencyCoordinator<K, V> consistencyCoordinator) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.indexName = Vldtn.requireNonNull(indexName, "indexName");
        this.staleLockRecovered = staleLockRecovered;
        this.startupAccess = Vldtn.requireNonNull(startupAccess,
                "startupAccess");
        this.stateCoordinator = Vldtn.requireNonNull(stateCoordinator,
                "stateCoordinator");
        this.consistencyCoordinator = Vldtn.requireNonNull(
                consistencyCoordinator, "consistencyCoordinator");
    }

    public void completeStartup(final Runnable startupConsistencyCheck) {
        final Runnable consistencyCheck = Vldtn.requireNonNull(
                startupConsistencyCheck, "startupConsistencyCheck");
        logger.debug("Opening index '{}'.", indexName);
        startupAccess.recoverFromWal();
        startupAccess.cleanupOrphanedSegmentDirectories();
        stateCoordinator.markReady();
        if (staleLockRecovered) {
            logger.info(STALE_LOCK_RECOVERY_MESSAGE);
            consistencyCoordinator.runStartupConsistencyCheck(consistencyCheck);
        }
        startupAccess.scheduleBackgroundSplitScan();
        logger.debug("Index '{}' opened.", indexName);
    }
}
