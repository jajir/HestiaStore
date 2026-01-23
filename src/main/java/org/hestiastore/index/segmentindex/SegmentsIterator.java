package org.hestiastore.index.segmentindex;

import java.util.List;
import java.util.NoSuchElementException;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
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

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final SegmentRegistry<K, V> segmentRegistry;
    private final List<SegmentId> ids;
    private final SegmentIteratorIsolation isolation;
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
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
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
            final Segment<K, V> segment;
            while (true) {
                final SegmentResult<Segment<K, V>> segmentResult = segmentRegistry
                        .getSegment(segmentId);
                if (segmentResult.getStatus() == SegmentResultStatus.BUSY) {
                    continue;
                }
                if (segmentResult.getStatus() != SegmentResultStatus.OK) {
                    throw new org.hestiastore.index.IndexException(String.format(
                            "Segment '%s' failed to load: %s", segmentId,
                            segmentResult.getStatus()));
                }
                segment = segmentResult.getValue();
                break;
            }
            while (true) {
                final SegmentResult<EntryIterator<K, V>> result = segment
                        .openIterator(isolation);
                if (result.getStatus() == SegmentResultStatus.OK) {
                    final EntryIterator<K, V> iterator = result.getValue();
                    if (iterator.hasNext()) {
                        currentIterator = iterator;
                        nextEntry = currentIterator.next();
                        return;
                    }
                    iterator.close();
                    break;
                }
                if (result.getStatus() == SegmentResultStatus.BUSY) {
                    continue;
                }
                throw new org.hestiastore.index.IndexException(String.format(
                        "Segment '%s' failed to open iterator: %s",
                        segment.getId(), result.getStatus()));
            }
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
