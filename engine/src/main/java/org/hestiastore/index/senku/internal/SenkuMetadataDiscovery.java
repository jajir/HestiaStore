package org.hestiastore.index.senku.internal;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;

/**
 * Coordinator-confined discovery of manifest-last publications. Committed
 * metadata is immutable under the exclusive writing lock, so writing scans
 * reuse parsed manifests. Strict scans reread every manifest at startup and
 * finalization. External mutation during writing is unsupported; disappearance
 * is still detected on discovery and changed contents are checked strictly.
 * Cached entries are bounded by the live sources, not the number ever written.
 */
final class SenkuMetadataDiscovery {

    private final Directory root;
    private final Directory flush;
    private final int shardCount;
    private Map<Long, Integer> flushes = new LinkedHashMap<>();
    private Map<String, SenkuRunSource> runs = new LinkedHashMap<>();

    SenkuMetadataDiscovery(final Directory root, final Directory flush,
            final int shardCount) {
        this.root = Vldtn.requireNonNull(root, "root");
        this.flush = Vldtn.requireNonNull(flush, "flush");
        this.shardCount = Vldtn.requireGreaterThanZero(shardCount,
                "shardCount");
    }

    /**
     * Reconciles a complete hierarchy observation, rereading known manifests
     * only for strict validation. Failed observations never replace the cache.
     *
     * @param catalog         coordinator-owned authoritative source catalog
     * @param reservedOutputs outputs not yet accepted by the coordinator
     * @param verifyKnown     whether all committed contents must be reread
     */
    void reconcile(final SenkuSourceCatalog catalog,
            final Set<String> reservedOutputs, final boolean verifyKnown) {
        final Map<Long, Integer> observedFlushes = scanFlushes(verifyKnown);
        final Map<String, SenkuRunSource> observedRuns = scanRuns(verifyKnown);
        catalog.reconcile(observedFlushes, observedRuns.values(),
                reservedOutputs);
        flushes = observedFlushes;
        runs = observedRuns;
    }

    /** Retains the already validated, successfully published worker result. */
    void rememberRun(final SenkuRunSource source) {
        runs.put(SenkuSourceCatalog.runPath(source), source);
    }

    /** Evicts a successfully deleted flush without waiting for another scan. */
    void forgetFlush(final long flushId) {
        flushes.remove(flushId);
    }

    /** Evicts a successfully deleted run without waiting for another scan. */
    void forgetRun(final SenkuRunSource source) {
        runs.remove(SenkuSourceCatalog.runPath(source));
    }

    private Map<Long, Integer> scanFlushes(final boolean verifyKnown) {
        final Map<Long, Integer> observed = new LinkedHashMap<>();
        for (final String name : flush.getFileNames().toList()) {
            final long id = SenkuFileNames.parseFlushDirectory(name);
            final Directory directory = flush.openSubDirectory(name);
            if (directory.isFileExists(SenkuFileNames.MANIFEST_FILE)) {
                final Integer cached = verifyKnown ? null : flushes.get(id);
                final int parts = cached == null
                        ? SenkuMetadataCodec.readFlushPartCount(directory)
                        : cached;
                if (observed.put(id, parts) != null) {
                    throw new IndexException("Duplicate flush ID " + id + ".");
                }
            }
        }
        return observed;
    }

    private Map<String, SenkuRunSource> scanRuns(final boolean verifyKnown) {
        final Map<String, SenkuRunSource> observed = new LinkedHashMap<>();
        for (final String name : root.getFileNames().toList()) {
            if (!SenkuFileNames.LOCK_FILE.equals(name)
                    && !SenkuFileNames.FLUSH_DIRECTORY.equals(name)
                    && !SenkuFileNames.FORMAT_FILE.equals(name)) {
                final int shardId = SenkuFileNames.parseShardDirectory(name);
                if (shardId >= shardCount) {
                    throw new IndexException(
                            "Unexpected shard ID " + shardId + ".");
                }
                scanShard(name, shardId, observed, verifyKnown);
            }
        }
        return observed;
    }

    private void scanShard(final String shardName, final int shardId,
            final Map<String, SenkuRunSource> observed,
            final boolean verifyKnown) {
        final Directory shard = root.openSubDirectory(shardName);
        for (final String levelName : shard.getFileNames().toList()) {
            final int level = SenkuFileNames.parseLevelDirectory(levelName);
            final Directory levelDirectory = shard.openSubDirectory(levelName);
            for (final String runName : levelDirectory.getFileNames()
                    .toList()) {
                final long runId = SenkuFileNames.parseRunDirectory(runName);
                final Directory directory = levelDirectory
                        .openSubDirectory(runName);
                if (directory.isFileExists(SenkuFileNames.MANIFEST_FILE)) {
                    final String path = shardName + "/" + levelName + "/"
                            + runName;
                    final SenkuRunSource cached = verifyKnown ? null
                            : runs.get(path);
                    final SenkuRunSource source = cached == null
                            ? new SenkuRunSource(directory, shardId, level,
                                    runId,
                                    SenkuMetadataCodec
                                            .readRunManifest(directory))
                            : cached;
                    if (observed.put(path, source) != null) {
                        throw new IndexException(
                                "Duplicate run source '" + path + "'.");
                    }
                }
            }
        }
    }
}
