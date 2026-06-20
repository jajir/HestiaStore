package org.hestiastore.index.segmentindex;

/**
 * Immutable value object that represents SQL-style pagination (OFFSET … LIMIT
 * …).
 *
 */
public final class SegmentWindow {

    static final SegmentWindow UNBOUNDED = new SegmentWindow(0,
            Integer.MAX_VALUE);
    private final int offset;
    private final int limit;

    private SegmentWindow(final int offset, final int limit) {
        this.offset = offset;
        this.limit = limit;
    }

    /**
     * Returns the limit as an integer, defaulting to {@link Integer#MAX_VALUE}
     * when unset.
     *
     * @return limit value or {@link Integer#MAX_VALUE}
     */
    public int getIntLimit() {
        return limit;
    }

    /**
     * Returns the offset as an integer, defaulting to 0 when unset.
     *
     * @return offset value or 0 when unset
     */
    public int getIntOffset() {
        return offset;
    }

    /** no OFFSET / no LIMIT (i.e. un-paginated) */
    public static SegmentWindow unbounded() {
        return UNBOUNDED;
    }

    /** only LIMIT n */
    public static SegmentWindow ofLimit(final int limit) {
        requireNonNegative(limit, "limit");
        return new SegmentWindow(0, limit);
    }

    /** only OFFSET n */
    public static SegmentWindow ofOffset(final int offset) {
        requireNonNegative(offset, "offset");
        return new SegmentWindow(offset, Integer.MAX_VALUE);
    }

    /** OFFSET + LIMIT */
    public static SegmentWindow of(final int offset, final int limit) {
        requireNonNegative(offset, "offset");
        requireNonNegative(limit, "limit");
        return new SegmentWindow(offset, limit);
    }

    private static void requireNonNegative(final int value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(
                    name + " must be >= 0 (was " + value + ")");
        }
    }

    /** true when neither OFFSET nor LIMIT is set */
    public boolean isUnbounded() {
        return offset == 0 && limit == Integer.MAX_VALUE;
    }
}
