package org.hestiastore.index.segmentindex;

import java.util.List;
import java.util.NoSuchElementException;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
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
    private Entry<K, V> currentEntry = null;
    private Entry<K, V> nextEntry = null;
    private EntryIterator<K, V> currentIterator = null;

    private int position = 0;

    SegmentsIterator(final List<SegmentId> ids,
            final SegmentRegistry<K, V> segmentRegistry) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.ids = Vldtn.requireNonNull(ids, "ids");
        nextSegmentIterator();
    }

    private void nextSegmentIterator() {
        if (currentIterator != null) {
            currentIterator.close();
            currentIterator = null;
        }
        if (position < ids.size()) {
            final SegmentId segmentId = ids.get(position);
            logger.debug("Starting processing segment '{}' which is {} of {}",
                    segmentId, position, ids.size());
            position++;
            final Segment<K, V> segment = segmentRegistry.getSegment(segmentId);
            currentIterator = segment.openIterator();
            if (currentIterator.hasNext()) {
                nextEntry = currentIterator.next();
            }
        }
    }

    @Override
    public boolean hasNext() {
        return nextEntry != null;
    }

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

    @Override
    protected void doClose() {
        if (currentIterator != null) {
            currentIterator.close();
        }
        currentIterator = null;
        nextEntry = null;
    }

}
