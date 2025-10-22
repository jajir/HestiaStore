package org.hestiastore.index.sst;

import java.util.List;
import java.util.NoSuchElementException;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
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
        implements PairIterator<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final SegmentRegistry<K, V> segmentRegistry;
    private final List<SegmentId> ids;
    private Pair<K, V> currentPair = null;
    private Pair<K, V> nextPair = null;
    private PairIterator<K, V> currentIterator = null;

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
                nextPair = currentIterator.next();
            }
        }
    }

    @Override
    public boolean hasNext() {
        return nextPair != null;
    }

    @Override
    public Pair<K, V> next() {
        if (nextPair == null) {
            throw new NoSuchElementException("There no next element.");
        }
        currentPair = nextPair;
        nextPair = null;
        if (currentIterator.hasNext()) {
            nextPair = currentIterator.next();
        } else {
            nextSegmentIterator();
        }
        return currentPair;
    }

    @Override
    protected void doClose() {
        if (currentIterator != null) {
            currentIterator.close();
        }
        currentIterator = null;
        nextPair = null;
    }

}
