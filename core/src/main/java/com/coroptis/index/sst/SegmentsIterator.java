package com.coroptis.index.sst;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import com.coroptis.index.ContextAwareLogger;
import com.coroptis.index.LoggingContext;
import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentId;

/**
 * Iterate through all segments in sst. It ignore main cache intentionally.
 * Class should not be exposed outside of package.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
class SegmentsIterator<K, V> implements PairIterator<K, V> {

    private final ContextAwareLogger logger;

    private final SegmentManager<K, V> segmentManager;
    private final List<SegmentId> ids;
    private Pair<K, V> currentPair = null;
    private Pair<K, V> nextPair = null;
    private PairIterator<K, V> currentIterator = null;

    private int position = 0;

    SegmentsIterator(final LoggingContext loggingContext,
            final List<SegmentId> ids,
            final SegmentManager<K, V> segmentManager) {
        this.logger = new ContextAwareLogger(SegmentsIterator.class,
                loggingContext);
        this.segmentManager = Objects.requireNonNull(segmentManager);
        this.ids = Objects.requireNonNull(ids);
        nextSegmentIterator();
    }

    private void nextSegmentIterator() {
        if (currentIterator != null) {
            currentIterator.close();
        }
        if (position < ids.size()) {
            final SegmentId segmentId = ids.get(position);
            logger.debug("Starting processing segment '{}' which is {} of {}",
                    segmentId, position, ids.size());
            position++;
            final Segment<K, V> segment = segmentManager.getSegment(segmentId);
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
    public void close() {
        if (currentIterator != null) {
            currentIterator.close();
        }
        currentIterator = null;
        nextPair = null;
        ids.clear();
    }

}
