package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

/**
 * Encapsulates the rules that determine when a segment should be compacted.
 * <p>
 * The policy depends only on {@link SegmentConf} thresholds and lightweight
 * {@link SegmentStats}. This keeps compaction eligibility testable without
 * touching the file-system heavy parts handled by {@link SegmentCompacter}.
 */
public final class SegmentCompactionPolicy {

    private final SegmentConf segmentConf;

    public SegmentCompactionPolicy(final SegmentConf segmentConf) {
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
    }

    /**
     * Decide whether compaction should run given current statistics.
     *
     * @param segmentStats current segment statistics (non-null)
     * @return {@code true} if the number of keys in delta cache exceeds the
     *         configured threshold
     */
    boolean shouldCompact(final SegmentStats segmentStats) {
        final SegmentStats stats = Vldtn.requireNonNull(segmentStats,
                "segmentStats");
        return stats.getNumberOfKeysInDeltaCache() > segmentConf
                .getMaxNumberOfKeysInDeltaCache();
    }

    /**
     * Decide whether compaction should run while data are being written.
     *
     * @param numberOfKeysInLastDeltaFile number of keys pending in the current
     *                                    delta cache file (must be
     *                                    non-negative)
     * @param segmentStats                current segment statistics
     * @return {@code true} if the accumulated keys would breach the write-time
     *         threshold
     */
    boolean shouldCompactDuringWriting(final long numberOfKeysInLastDeltaFile,
            final SegmentStats segmentStats) {
        if (numberOfKeysInLastDeltaFile < 0) {
            throw new IllegalArgumentException(
                    "numberOfKeysInLastDeltaFile must not be negative");
        }
        final SegmentStats stats = Vldtn.requireNonNull(segmentStats,
                "segmentStats");
        return stats.getNumberOfKeysInDeltaCache()
                + numberOfKeysInLastDeltaFile > segmentConf
                        .getMaxNumberOfKeysInDeltaCacheDuringWriting();
    }
}
