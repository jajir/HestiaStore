package org.hestiastore.index.segmentindex.split;

import org.hestiastore.index.Entry;
import org.hestiastore.index.segment.SegmentId;

/**
 * Handle for one offline-materialized segment that is not yet published into
 * the route map.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface PreparedSegmentHandle<K, V> extends AutoCloseable {

    /**
     * @return segment id assigned to the prepared segment
     */
    SegmentId segmentId();

    /**
     * Writes one entry into the prepared segment.
     *
     * @param entry entry to write
     */
    void write(Entry<K, V> entry);

    /**
     * Commits the prepared segment files.
     */
    void commit();

    /**
     * Discards prepared segment files.
     */
    void discard();

    /**
     * Releases open writer resources without publishing the segment.
     */
    @Override
    void close();
}
