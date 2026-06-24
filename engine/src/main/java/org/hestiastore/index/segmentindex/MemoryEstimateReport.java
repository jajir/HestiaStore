package org.hestiastore.index.segmentindex;

import java.util.List;
import java.util.OptionalLong;

import org.hestiastore.index.Vldtn;

/**
 * Immutable startup memory-estimation report.
 */
public final class MemoryEstimateReport {

    private final List<String> lines;
    private final boolean complete;
    private final OptionalLong totalEstimatedBytes;

    /**
     * Creates a memory-estimation report.
     *
     * @param lines user-facing report lines without log prefixes
     * @param complete whether all line items and totals were calculated
     * @param totalEstimatedBytes final estimate when complete
     */
    public MemoryEstimateReport(final List<String> lines,
            final boolean complete, final OptionalLong totalEstimatedBytes) {
        this.lines = List.copyOf(Vldtn.requireNonNull(lines, "lines"));
        this.complete = complete;
        this.totalEstimatedBytes = Vldtn.requireNonNull(totalEstimatedBytes,
                "totalEstimatedBytes");
    }

    /**
     * Returns whether all line items and totals were calculated.
     *
     * @return true when the estimate is complete
     */
    public boolean isComplete() {
        return complete;
    }

    /**
     * Returns final estimated bytes when available.
     *
     * @return final estimated bytes, or empty when the report is incomplete
     */
    public OptionalLong totalEstimatedBytes() {
        return totalEstimatedBytes;
    }

    /**
     * Returns user-facing report lines without log prefixes.
     *
     * @return immutable report lines
     */
    public List<String> lines() {
        return lines;
    }

    /**
     * Returns the report as one line-separator-delimited string.
     *
     * @return report text
     */
    public String text() {
        return String.join(System.lineSeparator(), lines);
    }

    @Override
    public String toString() {
        return text();
    }
}
