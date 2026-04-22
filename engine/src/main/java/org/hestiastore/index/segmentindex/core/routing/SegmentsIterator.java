package org.hestiastore.index.segmentindex.core.routing;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterate through all segments in sst. It ignore main cache intentionally.
 * Class should not be exposed outside of package.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
class SegmentsIterator<K, V> extends AbstractCloseableResource
        implements EntryIterator<K, V> {

    private static final IndexRetryPolicy DEFAULT_RETRY_POLICY = new IndexRetryPolicy(
            IndexConfigurationContract.DEFAULT_INDEX_BUSY_BACKOFF_MILLIS,
            IndexConfigurationContract.DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS);
    private static final String OPEN_ITERATOR_OPERATION = "openIterator";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentIteratorIsolation isolation;
    private final IndexRetryPolicy retryPolicy;
    private final List<SegmentId> ids;
    private Entry<K, V> currentEntry = null;
    private Entry<K, V> nextEntry = null;
    private EntryIterator<K, V> currentIterator = null;

    private int position = 0;

    SegmentsIterator(final List<SegmentId> ids,
            final SegmentRegistry<K, V> segmentRegistry) {
        this(ids, segmentRegistry, SegmentIteratorIsolation.FAIL_FAST);
    }

    SegmentsIterator(final List<SegmentId> ids,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentIteratorIsolation isolation) {
        this.ids = Vldtn.requireNonNull(ids, "ids");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.isolation = Vldtn.requireNonNull(isolation, "isolation");
        this.retryPolicy = DEFAULT_RETRY_POLICY;
        nextSegmentIterator();
    }

    private void nextSegmentIterator() {
        if (currentIterator != null) {
            currentIterator.close();
            currentIterator = null;
        }
        nextEntry = null;
        while (position < ids.size()) {
            final SegmentId segmentId = ids.get(position);
            logger.debug("Starting processing segment '{}' which is {} of {}",
                    segmentId, position, ids.size());
            position++;
            final EntryIterator<K, V> iterator = openSegmentIterator(segmentId);
            if (iterator != null && iterator.hasNext()) {
                currentIterator = iterator;
                nextEntry = currentIterator.next();
                return;
            }
            closeIterator(iterator);
        }
    }

    private static <K, V> void closeIterator(
            final EntryIterator<K, V> iterator) {
        if (iterator != null) {
            iterator.close();
        }
    }

    private EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId) {
        final SegmentHandle<K, V> segmentHandle = awaitSegment(segmentId);
        if (segmentHandle == null) {
            logger.debug(
                    "Skipping segment '{}' because it is not available in '{}' isolation mode.",
                    segmentId, isolation);
            return null;
        }
        final EntryIterator<K, V> iterator = awaitOpenIterator(segmentHandle,
                segmentId);
        if (iterator == null) {
            logger.debug(
                    "Skipping segment '{}' because iterator cannot be opened in '{}' isolation mode.",
                    segmentId, isolation);
        }
        return iterator;
    }

    private SegmentHandle<K, V> awaitSegment(final SegmentId segmentId) {
        if (isolation == SegmentIteratorIsolation.FAIL_FAST) {
            final Optional<SegmentHandle<K, V>> segment = segmentRegistry
                    .tryGetSegment(segmentId);
            return segment.orElse(null);
        }
        return segmentRegistry.loadSegment(segmentId);
    }

    private EntryIterator<K, V> awaitOpenIterator(
            final SegmentHandle<K, V> segmentHandle,
            final SegmentId segmentId) {
        if (isolation == SegmentIteratorIsolation.FAIL_FAST) {
            return awaitOpenFailFastIterator(segmentHandle, segmentId);
        }
        return segmentHandle.openIterator(isolation);
    }

    private EntryIterator<K, V> awaitOpenFailFastIterator(
            final SegmentHandle<K, V> segmentHandle,
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
        currentEntry = nextEntry;
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
        if (currentIterator != null) {
            currentIterator.close();
        }
        currentIterator = null;
        nextEntry = null;
    }

}
