package org.hestiastore.index.segmentindex;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;

/**
 * Blocking adapter over {@link SegmentRegistry} status-based API.
 * <p>
 * This adapter converts BUSY status responses to bounded retries governed by
 * {@link IndexRetryPolicy}.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentRegistryBlockingAdapter<K, V> {

    private final SegmentRegistry<K, V> segmentRegistry;
    private final IndexRetryPolicy retryPolicy;

    SegmentRegistryBlockingAdapter(final SegmentRegistry<K, V> segmentRegistry,
            final IndexRetryPolicy retryPolicy) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    Segment<K, V> awaitSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry
                    .getSegment(segmentId);
            if (loaded.getStatus() == SegmentRegistryResultStatus.OK
                    && loaded.getValue() != null) {
                return loaded.getValue();
            }
            if (loaded.getStatus() == SegmentRegistryResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "getSegment", segmentId);
                continue;
            }
            throw new IndexException(String.format(
                    "Segment '%s' failed to load: %s", segmentId,
                    loaded.getStatus()));
        }
    }

    void awaitDeleteSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentRegistryResult<Void> deleted = segmentRegistry
                    .deleteSegment(segmentId);
            if (deleted.getStatus() == SegmentRegistryResultStatus.OK
                    || deleted.getStatus() == SegmentRegistryResultStatus.CLOSED) {
                return;
            }
            if (deleted.getStatus() == SegmentRegistryResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "deleteSegment",
                        segmentId);
                continue;
            }
            throw new IndexException(String.format(
                    "Segment '%s' failed to delete: %s", segmentId,
                    deleted.getStatus()));
        }
    }
}
