package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

/**
 * Result of a segment build attempt.
 *
 * @param <T> built value type
 */
public final class SegmentBuildResult<T> {

    private final SegmentBuildStatus status;
    private final T value;

    private SegmentBuildResult(final SegmentBuildStatus status, final T value) {
        this.status = Vldtn.requireNonNull(status, "status");
        this.value = value;
    }

    /**
     * Creates an OK result with a value.
     *
     * @param value built value
     * @param <T>   result type
     * @return OK result
     */
    public static <T> SegmentBuildResult<T> ok(final T value) {
        return new SegmentBuildResult<>(SegmentBuildStatus.OK,
                Vldtn.requireNonNull(value, "value"));
    }

    /**
     * Creates a BUSY result.
     *
     * @param <T> result type
     * @return BUSY result
     */
    public static <T> SegmentBuildResult<T> busy() {
        return new SegmentBuildResult<>(SegmentBuildStatus.BUSY, null);
    }

    /**
     * Returns build status.
     *
     * @return status
     */
    public SegmentBuildStatus getStatus() {
        return status;
    }

    /**
     * Returns built value for OK results.
     *
     * @return value when status is OK
     * @throws IllegalStateException    when status is not OK
     * @throws IllegalArgumentException when status is OK but value is missing
     */
    public T getValue() {
        if (status != SegmentBuildStatus.OK) {
            throw new IllegalStateException(
                    String.format("Build result value is unavailable "
                            + "because segment build status "
                            + "is '%s'. Check getStatus() before "
                            + "calling getValue().", status));
        }
        return Vldtn.requireNonNull(value, "value");
    }

    /**
     * Returns true when status is OK.
     *
     * @return true for OK
     */
    public boolean isOk() {
        return status == SegmentBuildStatus.OK;
    }
}
