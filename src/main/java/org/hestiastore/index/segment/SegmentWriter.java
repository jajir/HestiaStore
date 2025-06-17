package org.hestiastore.index.segment;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;

/**
 * Allows to add data to segment. When searcher is in memory and number of added
 * keys doesn't exceed limit than it could work without invalidating cache and
 * searcher object..
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentWriter<K, V> implements PairWriter<K, V> {

    private final SegmentCompacter<K, V> segmentCompacter;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;

    /**
     * holds current delta cache writer.
     */
    private SegmentDeltaCacheWriter<K, V> deltaCacheWriter;

    public SegmentWriter(final SegmentCompacter<K, V> segmentCompacter,
            final SegmentDeltaCacheController<K, V> deltaCacheController) {
        this.segmentCompacter = Vldtn.requireNonNull(segmentCompacter,
                "segmentCompacter");
        this.deltaCacheController = Vldtn.requireNonNull(deltaCacheController,
                "deltaCacheController");
    }

    @Override
    public void close() {
        if (deltaCacheWriter != null) {
            deltaCacheWriter.close();
            deltaCacheWriter = null;
            segmentCompacter.optionallyCompact();
        }
    }

    @Override
    public void put(final Pair<K, V> pair) {
        optionallyOpenDeltaCacheWriter();
        deltaCacheWriter.put(pair);
        if (segmentCompacter.shouldBeCompactedDuringWriting(
                deltaCacheWriter.getNumberOfKeys())) {
            deltaCacheWriter.close();
            deltaCacheWriter = null;
            segmentCompacter.forceCompact();
        }
    }

    private void optionallyOpenDeltaCacheWriter() {
        if (deltaCacheWriter == null) {
            deltaCacheWriter = deltaCacheController.openWriter();
        }
    }

}
