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
public class SegmentDeltaCacheCompactingWriter<K, V>
        implements PairWriter<K, V> {

    private final SegmentImpl<K, V> segment;
    private final SegmentCompacter<K, V> segmentCompacter;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final SegmentCompactionPolicyWithManager compactionPolicy;

    /**
     * holds current delta cache writer.
     */
    private SegmentDeltaCacheWriter<K, V> deltaCacheWriter;

    public SegmentDeltaCacheCompactingWriter(
            final SegmentImpl<K, V> segment,
            final SegmentCompacter<K, V> segmentCompacter,
            final SegmentDeltaCacheController<K, V> deltaCacheController,
            final SegmentCompactionPolicyWithManager compactionPolicy) {
        this.segment = Vldtn.requireNonNull(segment, "segment");
        this.segmentCompacter = Vldtn.requireNonNull(segmentCompacter,
                "segmentCompacter");
        this.deltaCacheController = Vldtn.requireNonNull(deltaCacheController,
                "deltaCacheController");
        this.compactionPolicy = Vldtn.requireNonNull(compactionPolicy,
                "compactionPolicy");
    }

    @Override
    public void close() {
        if (deltaCacheWriter != null) {
            deltaCacheWriter.close();
            deltaCacheWriter = null;
            segmentCompacter.optionallyCompact(segment);
        }
    }

    @Override
    public void write(final Pair<K, V> pair) {
        optionallyOpenDeltaCacheWriter();
        deltaCacheWriter.write(pair);
        if (compactionPolicy
                .shouldCompactDuringWriting(deltaCacheWriter.getNumberOfKeys())) {
            deltaCacheWriter.close();
            deltaCacheWriter = null;
            segmentCompacter.forceCompact(segment);
        }
    }

    private void optionallyOpenDeltaCacheWriter() {
        if (deltaCacheWriter == null) {
            deltaCacheWriter = deltaCacheController.openWriter();
        }
    }

}
