package com.coroptis.index.sst;

import java.util.OptionalInt;

/**
 * Immutable value object that represents SQL-style pagination (OFFSET … LIMIT
 * …).
 *
 */
public class SegmentWindow {

    final static SegmentWindow UNBOUNDED = new SegmentWindow(
            OptionalInt.empty(), OptionalInt.empty());
    private final OptionalInt offset;
    private final OptionalInt limit;

    private SegmentWindow(final OptionalInt offset, final OptionalInt limit) {
        this.offset = offset;
        this.limit = limit;
    }

    public int getIntLimit() {
        return limit.orElse(Integer.MAX_VALUE);
    }

    public int getIntOffset() {
        return offset.orElse(0);
    }

    /** no OFFSET / no LIMIT (i.e. un-paginated) */
    public static SegmentWindow unbounded() {
        return UNBOUNDED;
    }

    /** only LIMIT n */
    public static SegmentWindow ofLimit(final int limit) {
        requireNonNegative(limit, "limit");
        return new SegmentWindow(OptionalInt.empty(), OptionalInt.of(limit));
    }

    /** only OFFSET n */
    public static SegmentWindow ofOffset(final int offset) {
        requireNonNegative(offset, "offset");
        return new SegmentWindow(OptionalInt.of(offset), OptionalInt.empty());
    }

    /** OFFSET + LIMIT */
    public static SegmentWindow of(final int offset, final int limit) {
        requireNonNegative(offset, "offset");
        requireNonNegative(limit, "limit");
        return new SegmentWindow(OptionalInt.of(offset), OptionalInt.of(limit));
    }

    private static void requireNonNegative(final int value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(
                    name + " must be ≥ 0 (was " + value + ")");
        }
    }

    /** true when neither OFFSET nor LIMIT is set */
    public boolean isUnbounded() {
        return offset.isEmpty() && limit.isEmpty();
    }
}
