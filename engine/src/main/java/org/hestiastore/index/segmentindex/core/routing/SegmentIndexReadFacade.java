package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;

/**
 * Owns iterator-oriented read operations on the index data path.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexReadFacade<K, V> {

    private final SegmentIndexTrackedOperationRunner<K, V> trackedRunner;
    private final SegmentIndexDataAccess<K, V> dataAccess;
    private final SegmentIndexEntryIteratorDecorator<K, V> iteratorDecorator;

    public SegmentIndexReadFacade(
            final SegmentIndexTrackedOperationRunner<K, V> trackedRunner,
            final SegmentIndexDataAccess<K, V> dataAccess,
            final SegmentIndexEntryIteratorDecorator<K, V> iteratorDecorator) {
        this.trackedRunner = Vldtn.requireNonNull(trackedRunner,
                "trackedRunner");
        this.dataAccess = Vldtn.requireNonNull(dataAccess, "dataAccess");
        this.iteratorDecorator = Vldtn.requireNonNull(iteratorDecorator,
                "iteratorDecorator");
    }

    public EntryIterator<K, V> openSegmentIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        return trackedRunner.runTracked(() -> dataAccess.openSegmentIterator(
                requireSegmentId(segmentId), requireIsolation(isolation)));
    }

    public EntryIterator<K, V> openWindowIterator(final SegmentWindow segmentWindow,
            final SegmentIteratorIsolation isolation) {
        return trackedRunner.runTracked(() -> iteratorDecorator.decorate(
                dataAccess.openWindowIterator(resolveSegmentWindow(segmentWindow),
                        requireIsolation(isolation))));
    }

    private SegmentId requireSegmentId(final SegmentId segmentId) {
        return Vldtn.requireNonNull(segmentId, "segmentId");
    }

    private SegmentIteratorIsolation requireIsolation(
            final SegmentIteratorIsolation isolation) {
        return Vldtn.requireNonNull(isolation, "isolation");
    }

    private SegmentWindow resolveSegmentWindow(
            final SegmentWindow segmentWindow) {
        return segmentWindow == null ? SegmentWindow.unbounded()
                : segmentWindow;
    }
}
