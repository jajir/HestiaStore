package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

/**
 * Bundles maintenance work into IO and publish phases.
 *
 * <p>The IO phase performs long-running disk work while the segment is in
 * MAINTENANCE. The publish phase runs after the gate transitions back to
 * FREEZE and should be short and memory-only, applying metadata/resource
 * updates before returning to READY.
 */
record SegmentMaintenanceWork(Runnable ioWork, Runnable publishWork) {

    SegmentMaintenanceWork {
        ioWork = Vldtn.requireNonNull(ioWork, "ioWork");
        publishWork = Vldtn.requireNonNull(publishWork, "publishWork");
    }
}
