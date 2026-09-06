package org.hestiastore.index.senku.internal;

import java.util.Optional;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.senku.SenkuLongKeySummary;

/**
 * Validated physical-part and logical-record counts for one sorted run.
 */
final class SenkuRunManifest {

    private final int partCount;
    private final long recordCount;
    private final Optional<SenkuLongKeySummary> longKeySummary;

    /**
     * Creates a validated run manifest.
     *
     * @param partCount   non-negative physical part count
     * @param recordCount non-negative logical record count
     */
    SenkuRunManifest(final int partCount, final long recordCount) {
        this(partCount, recordCount, Optional.empty());
    }

    /** Creates counts and an optional distribution of this committed output. */
    SenkuRunManifest(final int partCount, final long recordCount,
            final Optional<SenkuLongKeySummary> longKeySummary) {
        this.partCount = Vldtn.requireGreaterThanOrEqualToZero(partCount,
                "partCount");
        this.recordCount = Vldtn.requireGreaterThanOrEqualToZero(recordCount,
                "recordCount");
        if ((partCount == 0) != (recordCount == 0L)) {
            throw new IndexException(
                    "Run partCount and recordCount must both be zero or both be positive.");
        }
        this.longKeySummary = Vldtn.requireNonNull(longKeySummary,
                "longKeySummary");
        longKeySummary.ifPresent(summary -> Vldtn.requireTrue(
                summary.recordCount() == recordCount,
                "Run summary count must equal manifest recordCount"));
    }

    /**
     * Returns the exact physical part count.
     *
     * @return physical part count
     */
    int partCount() {
        return partCount;
    }

    /**
     * Returns the exact logical record count.
     *
     * @return logical record count
     */
    long recordCount() {
        return recordCount;
    }

    /** @return optional approximate natural-long output distribution */
    Optional<SenkuLongKeySummary> longKeySummary() {
        return longKeySummary;
    }
}
