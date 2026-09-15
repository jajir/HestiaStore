package org.hestiastore.index.senku.internal;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;

/**
 * Immutable catalog description of one committed sorted run.
 */
final class SenkuRunSource {

    private final Directory directory;
    private final int shardId;
    private final int level;
    private final long runId;
    private final SenkuRunManifest manifest;

    SenkuRunSource(final Directory directory, final int shardId,
            final int level, final long runId,
            final SenkuRunManifest manifest) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
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

    Directory directory() {
        return directory;
    }

    /**
     * Creates a lazily validated complete-run merge source.
     *
     * @param dataBlockSize chunk-store block size
     * @param maxEntriesPerPart configured physical part entry limit
     * @return merge source
     */
    SenkuMergeSource mergeSource(final DataBlockSize dataBlockSize,
            final long maxEntriesPerPart) {
        return SenkuMergeSource.run(new LargeFile(directory, dataBlockSize,
                maxEntriesPerPart, manifest.partCount()),
                manifest.recordCount());
    }
}
