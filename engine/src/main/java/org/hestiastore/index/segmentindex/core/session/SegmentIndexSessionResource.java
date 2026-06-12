package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.segmentindex.SegmentIndex;

/**
 * Internal segment-index instance owned by a session bootstrap flow.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentIndexSessionResource<K, V> extends SegmentIndex<K, V> {
}
