package org.hestiastore.index.segmentregistry;

import java.util.Optional;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * Internal adapter that exposes blocking and fail-fast facade operations over
 * the status-based registry protocol.
 * <p>
 * This adapter converts BUSY responses to bounded retries governed by
 * {@link BusyRetryPolicy}.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class BlockingSegmentRegistryAdapter<K, V> {

    private static final String SEGMENT_ID_PROPERTY = "segmentId";

    private final SegmentRegistryStatusAccess<K, V> segmentRegistry;
    private final BusyRetryPolicy retryPolicy;

    /**
     * Creates a blocking adapter over a status-based registry.
     *
     * @param segmentRegistry low-level registry
     * @param retryPolicy retry/backoff policy for BUSY responses
     */
    BlockingSegmentRegistryAdapter(
            final SegmentRegistryStatusAccess<K, V> segmentRegistry,
            final BusyRetryPolicy retryPolicy) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    Segment<K, V> getSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_PROPERTY);
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry
                    .tryGetSegment(segmentId);
            if (loaded.getStatus() == SegmentRegistryResultStatus.OK
                    && loaded.getValue() != null) {
                return loaded.getValue();
            }
            if (loaded.getStatus() == SegmentRegistryResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "getSegment",
                        segmentId);
                continue;
            }
            throw new IndexException(String.format(
                    "Segment '%s' failed to load: %s", segmentId,
                    loaded.getStatus()));
        }
    }

    Optional<Segment<K, V>> findSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_PROPERTY);
        final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry
                .tryGetSegment(segmentId);
        if (loaded.getStatus() == SegmentRegistryResultStatus.OK
                && loaded.getValue() != null) {
            return Optional.of(loaded.getValue());
        }
        if (loaded.getStatus() == SegmentRegistryResultStatus.BUSY
                || loaded.getStatus() == SegmentRegistryResultStatus.CLOSED) {
            return Optional.empty();
        }
        throw new IndexException(String.format(
                "Segment '%s' failed to load: %s", segmentId,
                loaded.getStatus()));
    }

    Segment<K, V> createSegment() {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentRegistryResult<Segment<K, V>> created = segmentRegistry
                    .tryCreateSegment();
            if (created.getStatus() == SegmentRegistryResultStatus.OK
                    && created.getValue() != null) {
                return created.getValue();
            }
            if (created.getStatus() == SegmentRegistryResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "createSegment", null);
                continue;
            }
            throw new IndexException(String.format(
                    "Segment failed to create: %s", created.getStatus()));
        }
    }

    void deleteSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_PROPERTY);
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentRegistryResult<Void> deleted = segmentRegistry
                    .tryDeleteSegment(segmentId);
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

    boolean deleteSegmentIfAvailable(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_PROPERTY);
        final SegmentRegistryResult<Void> deleted = segmentRegistry
                .tryDeleteSegment(segmentId);
        if (deleted.getStatus() == SegmentRegistryResultStatus.OK
                || deleted.getStatus() == SegmentRegistryResultStatus.CLOSED) {
            return true;
        }
        if (deleted.getStatus() == SegmentRegistryResultStatus.BUSY) {
            return false;
        }
        throw new IndexException(String.format(
                "Segment '%s' failed to delete: %s", segmentId,
                deleted.getStatus()));
    }
}
