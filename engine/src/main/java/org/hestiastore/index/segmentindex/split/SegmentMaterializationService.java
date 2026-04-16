package org.hestiastore.index.segmentindex.split;

import org.hestiastore.index.segment.SegmentId;

/**
 * Service for offline segment materialization used by split workflows.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentMaterializationService<K, V> {

    /**
     * Opens a new prepared segment handle backed by a freshly allocated segment
     * id.
     *
     * @return prepared segment handle ready for synchronous writes
     */
    PreparedSegmentHandle<K, V> openPreparedSegment();

    /**
     * Deletes files of a previously prepared segment.
     *
     * @param segmentId prepared segment id
     */
    void deletePreparedSegment(SegmentId segmentId);
}
