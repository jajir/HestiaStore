package org.hestiastore.index.senku.internal;

import org.hestiastore.index.Vldtn;

/**
 * Immutable successful merge-job result returned to the coordinator.
 */
final class SenkuCompletedRun {

    private final int shardId;
    private final int level;
    private final long runId;
    private final SenkuRunManifest manifest;

    SenkuCompletedRun(final int shardId, final int level, final long runId,
            final SenkuRunManifest manifest) {
        this.shardId = Vldtn.requireGreaterThanOrEqualToZero(shardId,
                "shardId");
        this.level = Vldtn.requireGreaterThanOrEqualToZero(level, "level");
        this.runId = Vldtn.requireGreaterThanOrEqualToZero(runId, "runId");
        this.manifest = Vldtn.requireNonNull(manifest, "manifest");
    }

    int shardId() {
        return shardId;
    }

    int level() {
        return level;
    }

    long runId() {
        return runId;
    }

    SenkuRunManifest manifest() {
        return manifest;
    }
}
