package com.coroptis.index.sst;

import java.util.Objects;
import java.util.stream.Stream;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.IndexException;
import com.coroptis.index.Pair;
import com.coroptis.index.log.LoggedKey;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

public interface Index<K, V> extends CloseableResource {

    static <M, N> IndexBuilder<M, N> builder() {
        return new IndexBuilder<>();
    }

    void put(K key, V value);

    default void put(final Pair<K, V> pair) {
        Objects.requireNonNull(pair, "Pair cant be null");
        put(pair.getKey(), pair.getValue());
    }

    V get(K key);

    void delete(K key);

    void compact();

    /**
     * Flush all data to disk. When WAL is used then it starts new file.
     */
    void flush();

    /**
     * Went through all records. In fact read all index data. Doesn't use
     * indexes and caches in segments.
     * 
     * @param segmentWindows allows to limit examined segments. If empty then
     *                       all segments are used.
     * @return stream of all data.
     */
    Stream<Pair<K, V>> getStream(SegmentWindow segmentWindows);

    UnsortedDataFileStreamer<LoggedKey<K>, V> getLogStreamer();

    /**
     * Checks the internal consistency of all index segments and associated data
     * descriptions.
     * <p>
     * This method traverses all segments and verifies that the index structure,
     * segment data, and metadata are valid and consistent. If correctable
     * inconsistencies are found, this method attempts to repair them
     * automatically. If an uncorrectable problem is detected, the method throws
     * an exception or signals failure, depending on the implementation.
     * <p>
     * <b>Typical consistency checks include:</b>
     * <ul>
     * <li>Validating segment structure and integrity</li>
     * <li>Checking for corrupt or missing metadata</li>
     * <li>Verifying key/value data descriptions are correct and complete</li>
     * <li>Ensuring no orphaned or unreachable segments</li>
     * </ul>
     * <p>
     * <b>Behavior:</b>
     * <ul>
     * <li>If all issues are correctable, the method repairs them and returns
     * normally.</li>
     * <li>If uncorrectable inconsistencies are found, the method throws an
     * {@code IndexException} or fails with an error.</li>
     * </ul>
     * <p>
     * <b>Implementation note:</b> Callers should invoke this method
     * periodically or after unexpected shutdowns to maintain data integrity.
     *
     * @throws IndexException if an uncorrectable inconsistency is detected.
     */
    void checkAndRepairConsistency();
}
