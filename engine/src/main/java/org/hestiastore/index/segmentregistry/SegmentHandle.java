package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segment.SegmentStats;

/**
 * Blocking facade for retry-aware access to a registry-managed segment.
 * <p>
 * The handle keeps the registry as the source of truth for loading/reloading a
 * segment and converts retryable segment-operation statuses into bounded
 * blocking calls.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentHandle<K, V> {

    /**
     * Returns this handle's logical segment id.
     *
     * @return segment id
     */
    SegmentId getId();

    /**
     * Returns the current loaded segment instance.
     * <p>
     * This is a temporary low-level escape hatch for callers that still need
     * direct access to the raw segment contract.
     *
     * @return current loaded segment
     */
    Segment<K, V> getSegment();

    /**
     * Returns runtime descriptors and tuning controls for this segment.
     *
     * @return runtime view
     */
    Runtime getRuntime();

    /**
     * Performs a single-attempt lookup without retrying BUSY/CLOSED.
     *
     * @param key key to look up
     * @return raw segment result
     */
    SegmentResult<V> tryGet(K key);

    /**
     * Performs a blocking lookup of a key in the segment.
     *
     * @param key key to look up
     * @return associated value or {@code null} when not present
     */
    V get(K key);

    /**
     * Performs a single-attempt write without retrying BUSY/CLOSED.
     *
     * @param key key to write
     * @param value value to write
     * @return raw segment result
     */
    SegmentResult<Void> tryPut(K key, V value);

    /**
     * Performs a blocking write into the segment.
     *
     * @param key key to write
     * @param value value to write
     */
    void put(K key, V value);

    /**
     * Opens an iterator in a single attempt without retrying BUSY/CLOSED.
     *
     * @param isolation iterator isolation
     * @return raw segment result
     */
    SegmentResult<EntryIterator<K, V>> tryOpenIterator(
            SegmentIteratorIsolation isolation);

    /**
     * Opens a blocking iterator using fail-fast isolation.
     *
     * @return iterator over segment entries
     */
    EntryIterator<K, V> openIterator();

    /**
     * Opens a blocking iterator with the requested isolation.
     *
     * @param isolation iterator isolation
     * @return iterator over segment entries
     */
    EntryIterator<K, V> openIterator(SegmentIteratorIsolation isolation);

    /**
     * Starts a flush in a single attempt without retrying BUSY/CLOSED.
     *
     * @return raw segment result
     */
    SegmentResult<Void> tryFlush();

    /**
     * Starts a flush and waits until the request is accepted.
     */
    void flush();

    /**
     * Starts compaction in a single attempt without retrying BUSY/CLOSED.
     *
     * @return raw segment result
     */
    SegmentResult<Void> tryCompact();

    /**
     * Starts a compaction pass and waits until the request is accepted.
     */
    void compact();

    /**
     * Validates the logical segment contents.
     *
     * @return last key in the segment, or {@code null} when empty
     */
    K checkAndRepairConsistency();

    /**
     * Invalidates active iterators for the segment.
     */
    void invalidateIterators();

    /**
     * Runtime descriptors and tuning controls for a segment handle.
     */
    interface Runtime {

        /**
         * Returns the current segment lifecycle state.
         *
         * @return current state
         */
        SegmentState getState();

        /**
         * Returns current statistics for the segment.
         *
         * @return immutable statistics snapshot
         */
        SegmentStats getStats();

        /**
         * Returns immutable runtime metrics for the segment.
         *
         * @return immutable runtime snapshot
         */
        SegmentRuntimeSnapshot getRuntimeSnapshot();

        /**
         * Returns the total number of keys visible in this segment.
         *
         * @return total number of keys
         */
        long getNumberOfKeys();

        /**
         * Returns the current number of keys buffered in the write cache.
         *
         * @return write-cache key count
         */
        int getNumberOfKeysInWriteCache();

        /**
         * Returns estimated total number of keys held by this segment,
         * including in-memory cache entries.
         *
         * @return estimated total number of cached keys
         */
        long getNumberOfKeysInCache();

        /**
         * Returns the number of keys currently held in the in-memory segment
         * cache.
         *
         * @return in-memory cache key count
         */
        long getNumberOfKeysInSegmentCache();

        /**
         * Returns the number of delta cache files recorded for this segment.
         *
         * @return number of delta cache files
         */
        int getNumberOfDeltaCacheFiles();

        /**
         * Applies runtime-only segment limits.
         *
         * @param limits runtime limits
         */
        void updateRuntimeLimits(SegmentRuntimeLimits limits);
    }
}
