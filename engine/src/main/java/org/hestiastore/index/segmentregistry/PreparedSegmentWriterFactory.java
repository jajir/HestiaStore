package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.segment.SegmentFullWriterTx;
import org.hestiastore.index.segment.SegmentId;

/**
 * Opens synchronous writer transactions for prepared segment materialization.
 *
 * @param <K> key type
 * @param <V> value type
 */
interface PreparedSegmentWriterFactory<K, V> {

    /**
     * Opens a synchronous bulk writer transaction for the provided segment id.
     *
     * @param segmentId segment id to materialize
     * @return full writer transaction for building the segment files
     */
    SegmentFullWriterTx<K, V> openWriterTx(SegmentId segmentId);
}
