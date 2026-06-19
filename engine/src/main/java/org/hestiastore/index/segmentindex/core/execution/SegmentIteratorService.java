package org.hestiastore.index.segmentindex.core.execution;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLease;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLeaseService;
import org.hestiastore.index.segmentindex.core.routing.RouteWindowSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides stable segment streaming operations.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIteratorService<K, V> {

    private static final String OPERATION_OPEN_FULL_ISOLATION_ITERATOR = "openFullIsolationIterator";
    private static final String OPEN_ITERATOR_OPERATION = "openIterator";

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SegmentIteratorService.class);

    private final MappedSegmentLeaseService<K, V> segmentLeaseService;
    private final BusyRetryPolicy retryPolicy;

    SegmentIteratorService(
            final MappedSegmentLeaseService<K, V> segmentLeaseService,
            final BusyRetryPolicy retryPolicy) {
        this.segmentLeaseService = Vldtn.requireNonNull(segmentLeaseService,
                "segmentLeaseService");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    /**
     * Creates a segment streaming service with its retry policy.
     *
     * @param segmentLeaseService route and segment lease service
     * @param busyBackoffMillis retry backoff in milliseconds
     * @param busyTimeoutMillis retry timeout in milliseconds
     * @param <M> key type
     * @param <N> value type
     * @return segment streaming service
     */
    public static <M, N> SegmentIteratorService<M, N> create(
            final MappedSegmentLeaseService<M, N> segmentLeaseService,
            final int busyBackoffMillis,
            final int busyTimeoutMillis) {
        return new SegmentIteratorService<>(
                Vldtn.requireNonNull(segmentLeaseService,
                        "segmentLeaseService"),
                new BusyRetryPolicy(busyBackoffMillis, busyTimeoutMillis,
                        "Streaming operation"));
    }

    /**
     * Opens an iterator against one stable segment, retrying transient busy
     * states.
     *
     * @param segmentId segment id
     * @param isolation iterator isolation mode
     * @return entry iterator
     */
    public EntryIterator<K, V> openIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final MappedSegmentLease<K, V> lease = segmentLeaseService
                    .acquireMappedSegment(segmentId);
            if (lease == null) {
                retryPolicy.backoffOrThrow(startNanos, OPEN_ITERATOR_OPERATION,
                        segmentId);
                continue;
            }
            try {
                final OperationResult<EntryIterator<K, V>> result =
                        lease.segment().tryOpenIterator(isolation);
                if (result.getStatus() == OperationStatus.OK) {
                    return result.getValue();
                }
                if (result.getStatus() == OperationStatus.BUSY) {
                    retryPolicy.backoffOrThrow(startNanos,
                            OPEN_ITERATOR_OPERATION, segmentId);
                    continue;
                }
                throw new IndexException(String.format(
                        "Index operation '%s' failed on segment '%s': %s",
                        OPEN_ITERATOR_OPERATION, segmentId,
                        result.getStatus()));
            } finally {
                lease.close();
            }
        }
    }

    /**
     * Opens an iterator over a resolved segment window.
     *
     * @param resolvedWindows segment window already resolved for the caller
     * @param isolation iterator isolation mode
     * @return entry iterator
     */
    public EntryIterator<K, V> openWindowIterator(
            final SegmentWindow resolvedWindows,
            final SegmentIteratorIsolation isolation) {
        final SegmentWindow nonNullWindow = Vldtn.requireNonNull(
                resolvedWindows, "resolvedWindows");
        final SegmentIteratorIsolation nonNullIsolation = Vldtn.requireNonNull(
                isolation, "isolation");
        if (nonNullIsolation == SegmentIteratorIsolation.FULL_ISOLATION) {
            return openStableIteratorWithRouteSnapshot(nonNullWindow,
                    nonNullIsolation);
        }
        return openStableIterator(segmentLeaseService.getSegmentIds(
                nonNullWindow), nonNullIsolation);
    }

    private EntryIterator<K, V> openStableIteratorWithRouteSnapshot(
            final SegmentWindow resolvedWindows,
            final SegmentIteratorIsolation isolation) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final RouteWindowSnapshot snapshot = segmentLeaseService
                    .snapshotSegmentIds(resolvedWindows);
            final EntryIterator<K, V> iterator = openStableIterator(
                    snapshot.segmentIds(), isolation);
            if (segmentLeaseService.isCurrent(snapshot)) {
                return iterator;
            }
            iterator.close();
            retryPolicy.backoffOrThrow(startNanos,
                    OPERATION_OPEN_FULL_ISOLATION_ITERATOR, null);
        }
    }

    private EntryIterator<K, V> openStableIterator(
            final List<SegmentId> segmentIds,
            final SegmentIteratorIsolation isolation) {
        return new StableSegmentsIterator<>(segmentIds, segmentLeaseService,
                isolation);
    }

    /**
     * Invalidates iterators for currently mapped loaded stable segments.
     */
    public void invalidateIterators() {
        segmentLeaseService.getLoadedMappedSegmentIds()
                .forEach(this::invalidateIteratorsForSegment);
    }

    private void invalidateIteratorsForSegment(final SegmentId segmentId) {
        try {
            handleLoadedSegment(segmentId, segmentLeaseService
                    .tryAcquireLoadedMappedSegment(segmentId));
        } catch (final IndexException e) {
            logIteratorInvalidationLookupFailure(segmentId, e);
        }
    }

    private void handleLoadedSegment(final SegmentId segmentId,
            final Optional<MappedSegmentLease<K, V>> loadedSegment) {
        if (loadedSegment.isPresent()) {
            try (MappedSegmentLease<K, V> lease = loadedSegment.get()) {
                lease.segment().invalidateIterators();
            }
            return;
        }
        logMissingSegment(segmentId);
    }

    private void logMissingSegment(final SegmentId segmentId) {
        LOGGER.debug(
                "Skipping iterator invalidation for segment '{}' because it is "
                        + "not immediately available.",
                segmentId);
    }

    private void logIteratorInvalidationLookupFailure(final SegmentId segmentId,
            final IndexException exception) {
        LOGGER.debug(
                "Skipping iterator invalidation for segment '{}' because segment lease lookup failed.",
                segmentId, exception);
    }
}
