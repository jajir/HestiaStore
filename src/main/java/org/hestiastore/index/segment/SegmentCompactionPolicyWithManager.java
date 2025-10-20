package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

/**
 * Convenience wrapper around {@link SegmentCompactionPolicy} that reads the
 * current {@link SegmentStats} from a bound {@link SegmentPropertiesManager} so
 * callers don't need to pass stats explicitly.
 * <p>
 * This keeps {@code SegmentCompactionPolicy} pure and easy to test while
 * providing an ergonomic facade where a single manager instance is available.
 */
public final class SegmentCompactionPolicyWithManager {

    private final SegmentCompactionPolicy policy;
    private final SegmentPropertiesManager propertiesManager;

    /**
     * Creates a wrapper that uses the given policy and properties manager.
     *
     * @param policy            immutable compaction rules based on conf
     * @param propertiesManager source of live segment statistics
     */
    public SegmentCompactionPolicyWithManager(
            final SegmentCompactionPolicy policy,
            final SegmentPropertiesManager propertiesManager) {
        this.policy = Vldtn.requireNonNull(policy, "policy");
        this.propertiesManager = Vldtn.requireNonNull(propertiesManager,
                "propertiesManager");
    }

    /**
     * Decides whether compaction should run using the most recent segment
     * statistics from {@link SegmentPropertiesManager}.
     *
     * @return true if compaction is recommended based on current stats
     */
    public boolean shouldCompact() {
        final SegmentStats stats = propertiesManager.getSegmentStats();
        return policy.shouldCompact(stats);
    }

    /**
     * Decides whether compaction should run during writing using the most
     * recent segment statistics from {@link SegmentPropertiesManager}.
     *
     * @param numberOfKeysInLastDeltaFile number of keys pending in last delta
     *                                    file
     * @return true if compaction is recommended during writing
     * @throws IllegalArgumentException when {@code numberOfKeysInLastDeltaFile}
     *                                  is negative
     */
    public boolean shouldCompactDuringWriting(
            final long numberOfKeysInLastDeltaFile) {
        final SegmentStats stats = propertiesManager.getSegmentStats();
        return policy.shouldCompactDuringWriting(numberOfKeysInLastDeltaFile,
                stats);
    }

    /**
     * Factory method to construct a wrapped policy directly from configuration
     * and a properties manager.
     */
    public static SegmentCompactionPolicyWithManager from(
            final SegmentConf conf,
            final SegmentPropertiesManager propertiesManager) {
        return new SegmentCompactionPolicyWithManager(
                new SegmentCompactionPolicy(
                        Vldtn.requireNonNull(conf, "segmentConf")),
                Vldtn.requireNonNull(propertiesManager, "propertiesManager"));
    }
}
