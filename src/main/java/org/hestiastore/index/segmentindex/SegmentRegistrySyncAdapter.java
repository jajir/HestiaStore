package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * Adapter that retries BUSY registry responses using {@link IndexRetryPolicy}.
 *
 * <p>
 * This wrapper does not add synchronization. It simply retries
 * {@link SegmentRegistry#getSegment(SegmentId)} until a non-BUSY status is
 * returned or the retry policy times out.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentRegistrySyncAdapter<K, V> implements SegmentRegistry<K, V> {

    private final SegmentRegistry<K, V> delegate;
    private final IndexRetryPolicy retryPolicy;

    SegmentRegistrySyncAdapter(final SegmentRegistry<K, V> delegate,
            final IndexRetryPolicy retryPolicy) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    /**
     * Returns the segment for the provided id, retrying while BUSY.
     *
     * @param segmentId segment id to load
     * @return result containing the segment or a status
     */
    @Override
    public SegmentRegistryResult<Segment<K, V>> getSegment(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentRegistryResult<Segment<K, V>> result = delegate
                    .getSegment(segmentId);
            if (result.getStatus() == SegmentRegistryResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "getSegment",
                        segmentId);
                continue;
            }
            return result;
        }
    }

    /**
     * Removes a segment from the registry and deletes its files.
     *
     * @param segmentId segment id to remove
     */
    @Override
    public void removeSegment(final SegmentId segmentId) {
        delegate.removeSegment(segmentId);
    }

    /**
     * Closes the registry, releasing cached segments and executors.
     */
    @Override
    public void close() {
        delegate.close();
    }
}
