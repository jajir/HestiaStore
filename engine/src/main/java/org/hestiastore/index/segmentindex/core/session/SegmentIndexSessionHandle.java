package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.segmentindex.SegmentIndex;

/**
 * Core-session base type for live segment-index handles.
 *
 * @param <K> key type
 * @param <V> value type
 */
public abstract class SegmentIndexSessionHandle<K, V>
        extends AbstractCloseableResource implements SegmentIndex<K, V> {
}
