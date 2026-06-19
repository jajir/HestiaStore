package org.hestiastore.index.segmentindex.core.execution;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfigurationDefaults;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLease;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLeaseService;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterates through stable segments directly and intentionally ignores the main
 * cache.
 *
 * @param <K> key type
 * @param <V> value type
 */
class StableSegmentsIterator<K, V> extends AbstractCloseableResource
        implements EntryIterator<K, V> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(StableSegmentsIterator.class);
    private static final BusyRetryPolicy DEFAULT_RETRY_POLICY = new BusyRetryPolicy(
            IndexConfigurationDefaults.DEFAULT_INDEX_BUSY_BACKOFF_MILLIS,
            IndexConfigurationDefaults.DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS,
            "Streaming operation");
    private static final String OPEN_ITERATOR_OPERATION = "openIterator";

    private final MappedSegmentLeaseService<K, V> segmentLeaseService;
    private final SegmentIteratorIsolation isolation;
    private final BusyRetryPolicy retryPolicy;
    private final List<SegmentId> ids;
    private Entry<K, V> nextEntry = null;
    private EntryIterator<K, V> currentIterator = null;

    private int position = 0;

    StableSegmentsIterator(final List<SegmentId> ids,
            final MappedSegmentLeaseService<K, V> segmentLeaseService) {
        this(ids, segmentLeaseService, SegmentIteratorIsolation.FAIL_FAST);
    }

    StableSegmentsIterator(final List<SegmentId> ids,
            final MappedSegmentLeaseService<K, V> segmentLeaseService,
            final SegmentIteratorIsolation isolation) {
        this.ids = Vldtn.requireNonNull(ids, "ids");
        this.segmentLeaseService = Vldtn.requireNonNull(segmentLeaseService,
                "segmentLeaseService");
        this.isolation = Vldtn.requireNonNull(isolation, "isolation");
        this.retryPolicy = DEFAULT_RETRY_POLICY;
        nextSegmentIterator();
    }

    private void nextSegmentIterator() {
        closeCurrentSegment();
        nextEntry = null;
        while (position < ids.size()) {
            final SegmentId segmentId = ids.get(position);
            LOGGER.debug("Starting processing segment '{}' which is {} of {}",
                    segmentId, position, ids.size());
            position++;
            final EntryIterator<K, V> iterator = openSegmentIterator(segmentId);
            if (iterator == null) {
                continue;
            }
            try {
                if (iterator.hasNext()) {
                    nextEntry = iterator.next();
                    currentIterator = iterator;
                    return;
                }
            } catch (final RuntimeException e) {
                iterator.close();
                throw e;
            }
            iterator.close();
        }
    }

    private void closeCurrentSegment() {
        if (currentIterator == null) {
            return;
        }
        final EntryIterator<K, V> iterator = currentIterator;
        currentIterator = null;
        iterator.close();
    }

    private EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId) {
        final MappedSegmentLease<K, V> lease = acquireSegment(segmentId);
        if (lease == null) {
            LOGGER.debug(
                    "Skipping segment '{}' because it is not available in '{}' isolation mode.",
                    segmentId, isolation);
            return null;
        }
        EntryIterator<K, V> iterator = null;
        try {
            iterator = awaitOpenIterator(lease.segment(), segmentId);
            if (iterator == null) {
                LOGGER.debug(
                        "Skipping segment '{}' because iterator cannot be opened in '{}' isolation mode.",
                        segmentId, isolation);
                return null;
            }
            return iterator;
        } catch (final RuntimeException e) {
            if (iterator != null) {
                iterator.close();
            }
            throw e;
        } finally {
            lease.close();
        }
    }

    private MappedSegmentLease<K, V> acquireSegment(final SegmentId segmentId) {
        if (isolation == SegmentIteratorIsolation.FAIL_FAST) {
            final Optional<MappedSegmentLease<K, V>> lease = segmentLeaseService
                    .tryAcquireMappedSegment(segmentId);
            return lease.orElse(null);
        }
        return segmentLeaseService.acquireMappedSegment(segmentId);
    }

    private EntryIterator<K, V> awaitOpenIterator(
            final BlockingSegment<K, V> segmentHandle,
            final SegmentId segmentId) {
        if (isolation == SegmentIteratorIsolation.FAIL_FAST) {
            return awaitOpenFailFastIterator(segmentHandle, segmentId);
        }
        return segmentHandle.openIterator(isolation);
    }

    private EntryIterator<K, V> awaitOpenFailFastIterator(
            final BlockingSegment<K, V> segmentHandle,
            final SegmentId segmentId) {
        final long startNanos = retryPolicy.startNanos();
        for (int attempt = 0; attempt < 2; attempt++) {
            final OperationResult<EntryIterator<K, V>> result = segmentHandle
                    .tryOpenIterator(isolation);
            if (result.getStatus() == OperationStatus.OK
                    && result.getValue() != null) {
                return result.getValue();
            }
            if (result.getStatus() != OperationStatus.BUSY) {
                return null;
            }
            if (attempt == 0) {
                retryPolicy.backoffOrThrow(startNanos,
                        OPEN_ITERATOR_OPERATION, segmentId);
            }
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        return nextEntry != null;
    }

    /** {@inheritDoc} */
    @Override
    public Entry<K, V> next() {
        if (nextEntry == null) {
            throw new NoSuchElementException("There no next element.");
        }
        final Entry<K, V> currentEntry = nextEntry;
        nextEntry = null;
        if (currentIterator.hasNext()) {
            nextEntry = currentIterator.next();
        } else {
            nextSegmentIterator();
        }
        return currentEntry;
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        closeCurrentSegment();
        nextEntry = null;
    }

}
