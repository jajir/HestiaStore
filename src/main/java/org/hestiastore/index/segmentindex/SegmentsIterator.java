package org.hestiastore.index.segmentindex;

import java.util.List;
import java.util.NoSuchElementException;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
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

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final SegmentRegistryBlockingAdapter<K, V> segmentRegistryBlockingAdapter;
    private final IndexRetryPolicy retryPolicy;
    private final List<SegmentId> ids;
    private final SegmentIteratorIsolation isolation;
    private Entry<K, V> currentEntry = null;
    private Entry<K, V> nextEntry = null;
    private EntryIterator<K, V> currentIterator = null;

    private int position = 0;

    SegmentsIterator(final List<SegmentId> ids,
            final SegmentRegistry<K, V> segmentRegistry) {
        this(ids, segmentRegistry, SegmentIteratorIsolation.FAIL_FAST,
                DEFAULT_RETRY_POLICY);
    }

    SegmentsIterator(final List<SegmentId> ids,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentIteratorIsolation isolation) {
        this(ids, segmentRegistry, isolation, DEFAULT_RETRY_POLICY);
    }

    SegmentsIterator(final List<SegmentId> ids,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentIteratorIsolation isolation,
            final IndexRetryPolicy retryPolicy) {
        this.segmentRegistryBlockingAdapter = new SegmentRegistryBlockingAdapter<>(
                segmentRegistry, retryPolicy);
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        this.ids = Vldtn.requireNonNull(ids, "ids");
        this.isolation = Vldtn.requireNonNull(isolation, "isolation");
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
            final Segment<K, V> segment = segmentRegistryBlockingAdapter
                    .awaitSegment(segmentId);
            final EntryIterator<K, V> iterator = awaitOpenIterator(segment);
            if (iterator.hasNext()) {
                currentIterator = iterator;
                nextEntry = currentIterator.next();
                return;
            }
            iterator.close();
        }
    }

    private EntryIterator<K, V> awaitOpenIterator(final Segment<K, V> segment) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentResult<EntryIterator<K, V>> result = segment
                    .openIterator(isolation);
            if (result.getStatus() == SegmentResultStatus.OK
                    && result.getValue() != null) {
                return result.getValue();
            }
            if (result.getStatus() == SegmentResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "openIterator",
                        segment.getId());
                continue;
            }
            throw new IndexException(String.format(
                    "Segment '%s' failed to open iterator: %s", segment.getId(),
                    result.getStatus()));
        }
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
