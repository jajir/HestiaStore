package com.coroptis.index.sst;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentFiles;
import com.coroptis.index.segment.SegmentId;
import com.coroptis.index.segment.SegmentReader;

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

    private final SstIndexImpl<K, V> sstIndex;
    private final List<SegmentId> ids;
    private Pair<K, V> currentPair = null;
    private Pair<K, V> nextPair = null;
    private PairIterator<K, V> currentIterator = null;

    SegmentsIterator(final SstIndexImpl<K, V> sstIndex) {
        this.sstIndex = Objects.requireNonNull(sstIndex);
        ids = new ArrayList<>(sstIndex.getSegmentIds());
        nextSegmentIterator();
    }

    private void nextSegmentIterator() {
        if (currentIterator != null) {
            currentIterator.close();
        }
        if (!ids.isEmpty()) {
            final SegmentId segmentId = ids.remove(0);
            // is segment in memory?
            final SegmentManager<K, V> segmentManager = sstIndex
                    .getSegmentManager();
            if (segmentManager.isInCache(segmentId)) {
                currentIterator = sstIndex.openSegmentIterator(segmentId);
                if (currentIterator.hasNext()) {
                    nextPair = currentIterator.next();
                }
            } else {
                final SegmentFiles<K, V> segmentFiles = segmentManager
                        .getSegmentFiles(segmentId);
                final SegmentReader<K, V> segmentReader = new SegmentReader<>(
                        segmentFiles);
                currentIterator = segmentReader.openIterator(() -> {
                    if (segmentManager.isInCache(segmentId)) {
                        final Segment<K, V> segment = segmentManager
                                .getSegment(segmentId);
                        return segment.getVersion();
                    } else {
                        return 0;
                    }
                });
                if (currentIterator.hasNext()) {
                    nextPair = currentIterator.next();
                }
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
    public Optional<Pair<K, V>> readCurrent() {
        return Optional.ofNullable(currentPair);
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
