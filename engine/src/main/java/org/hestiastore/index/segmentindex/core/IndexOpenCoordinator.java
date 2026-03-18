package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.Vldtn;
import org.slf4j.Logger;

/**
 * Owns the startup sequence after runtime collaborators are assembled.
 */
final class IndexOpenCoordinator {

    private static final String STALE_LOCK_RECOVERY_MESSAGE = "Recovered stale index lock (.lock). Index is going to be checked for consistency and unlocked.";

    private final Logger logger;
    private final String indexName;

    IndexOpenCoordinator(final Logger logger, final String indexName) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.indexName = Vldtn.requireNonNull(indexName, "indexName");
    }

    void completeOpen(final boolean staleLockRecovered,
            final Runnable recoverWal,
            final Runnable cleanupOrphanedSegmentDirectories,
            final Runnable markReady, final Runnable runStartupConsistencyCheck,
            final Runnable scheduleBackgroundSplitScan) {
        Vldtn.requireNonNull(recoverWal, "recoverWal");
        Vldtn.requireNonNull(cleanupOrphanedSegmentDirectories,
                "cleanupOrphanedSegmentDirectories");
        Vldtn.requireNonNull(markReady, "markReady");
        Vldtn.requireNonNull(runStartupConsistencyCheck,
                "runStartupConsistencyCheck");
        Vldtn.requireNonNull(scheduleBackgroundSplitScan,
                "scheduleBackgroundSplitScan");
        logger.debug("Opening index '{}'.", indexName);
        recoverWal.run();
        cleanupOrphanedSegmentDirectories.run();
        markReady.run();
        if (staleLockRecovered) {
            logger.info(STALE_LOCK_RECOVERY_MESSAGE);
            runStartupConsistencyCheck.run();
        }
        scheduleBackgroundSplitScan.run();
        logger.debug("Index '{}' opened.", indexName);
    }
}
