package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

/**
 * Bundles maintenance work into IO and publish phases.
 *
 * <p>The IO phase performs long-running disk work while the segment is in
 * MAINTENANCE. The publish phase runs after the gate transitions back to
 * FREEZE and should be short, applying metadata/resource updates before
 * returning to READY.
 */
final class SegmentMaintenanceWork {

    private final Runnable ioWork;
    private final Runnable publishWork;

    /**
     * Creates a maintenance work bundle.
     *
     * @param ioWork IO phase runnable
     * @param publishWork publish phase runnable
     */
    SegmentMaintenanceWork(final Runnable ioWork,
            final Runnable publishWork) {
        this.ioWork = Vldtn.requireNonNull(ioWork, "ioWork");
        this.publishWork = Vldtn.requireNonNull(publishWork, "publishWork");
    }

    /**
     * Returns the IO phase task.
     *
     * @return IO phase runnable
     */
    Runnable ioWork() {
        return ioWork;
    }

    /**
     * Returns the publish phase task.
     *
     * @return publish phase runnable
     */
    Runnable publishWork() {
        return publishWork;
    }
}
