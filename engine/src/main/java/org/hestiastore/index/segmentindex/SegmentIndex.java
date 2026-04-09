package org.hestiastore.index.segmentindex;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.core.SegmentIndexFactory;

/**
 * High-level contract for the segment-index layer that sits above individual
 * segments. It supports creating/opening instances, point mutations
 * (put/delete), range streaming, log inspection, and consistency checks.
 * Concrete implementations are created through the static factory helpers which
 * take care of configuration handling and bounded directory wrapping.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentIndex<K, V> extends CloseableResource {
    /**
     * Creates a brand new index in the supplied directory. Default values are
     * applied to the provided configuration, then both the configuration and
     * the on-disk structures are persisted.
     * <p>
     * The returned index owns the supplied directory facade and will close it
     * when the index is closed if it implements {@link CloseableResource}.
     * </p>
     *
     * @param directory backing directory for the index
     * @param indexConf requested configuration overrides
     * @return a newly created index instance
     */
    static <M, N> SegmentIndex<M, N> create(final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        return SegmentIndexFactory.create(directory, indexConf);
    }

    static <M, N> SegmentIndex<M, N> create(final Directory directory,
            final IndexConfiguration<M, N> indexConf,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        return SegmentIndexFactory.create(directory, indexConf,
                chunkFilterProviderRegistry);
    }

    /**
     * Opens an existing index, merging the provided configuration overrides
     * with the stored configuration on disk.
     * <p>
     * The returned index owns the supplied directory facade and will close it
     * when the index is closed if it implements {@link CloseableResource}.
     * </p>
     *
     * @param directory backing directory that already contains the index
     * @param indexConf configuration overrides to apply
     * @return index instance backed by the updated configuration
     */
    static <M, N> SegmentIndex<M, N> open(final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        return SegmentIndexFactory.open(directory, indexConf);
    }

    static <M, N> SegmentIndex<M, N> open(final Directory directory,
            final IndexConfiguration<M, N> indexConf,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        return SegmentIndexFactory.open(directory, indexConf,
                chunkFilterProviderRegistry);
    }

    /**
     * Opens an existing index using the configuration stored on disk.
     * <p>
     * The returned index owns the supplied directory facade and will close it
     * when the index is closed if it implements {@link CloseableResource}.
     * </p>
     *
     * @param directory backing directory with an existing index
     * @return index instance backed by the persisted configuration
     */
    static <M, N> SegmentIndex<M, N> open(final Directory directory) {
        return SegmentIndexFactory.open(directory);
    }

    static <M, N> SegmentIndex<M, N> open(final Directory directory,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        return SegmentIndexFactory.open(directory, chunkFilterProviderRegistry);
    }

    /**
     * Attempts to open an index when it may or may not exist.
     * <p>
     * The returned index owns the supplied directory facade and will close it
     * when the index is closed if it implements {@link CloseableResource}.
     * </p>
     *
     * @param directory backing directory that may contain an index
     * @return optional index instance if the configuration was found
     */
    static <M, N> Optional<SegmentIndex<M, N>> tryOpen(
            final Directory directory) {
        return SegmentIndexFactory.tryOpen(directory);
    }

    static <M, N> Optional<SegmentIndex<M, N>> tryOpen(
            final Directory directory,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        return SegmentIndexFactory.tryOpen(directory,
                chunkFilterProviderRegistry);
    }

    /**
     * Inserts or updates a single entry in the index.
     *
     * @param key   key to write (must not be null)
     * @param value value to associate with the key (must not be null and must
     *              not be a tombstone value)
     */
    void put(K key, V value);

    /**
     * Convenience overload that accepts a pre-built {@link Entry}.
     *
     * @param entry key/value pair to write
     */
    default void put(final Entry<K, V> entry) {
        Vldtn.requireNonNull(entry, "entry");
        put(entry.getKey(), entry.getValue());
    }

    /**
     * Performs a point lookup for the given key.
     *
     * @param key key to search for
     * @return stored value or {@code null} when no entry exists
     */
    V get(K key);

    /**
     * Deletes (tombstones) the provided key.
     *
     * @param key key to remove from the index
     */
    void delete(K key);

    /**
     * Starts a compaction pass over in-memory and on-disk data structures. This
     * is the explicit index-level entry point for compaction; automatic split
     * logic does not invoke compaction.
     *
     * The call returns after compaction is accepted by each segment.
     */
    void compact();

    /**
     * Starts flushing in-memory data to disk. The call returns after flush is
     * accepted by each segment.
     */
    void flush();

    /**
     * Starts a compaction pass and waits until all segment maintenance
     * operations complete. Do not call from a segment maintenance executor
     * thread.
     *
     * This is the explicit index-level entry point for compaction; automatic
     * split logic does not invoke compaction.
     */
    void compactAndWait();

    /**
     * Starts flushing in-memory data to disk and waits until all segment
     * maintenance operations complete. Do not call from a segment maintenance
     * executor thread.
     */
    void flushAndWait();

    /**
     * Went through all records. In fact read all index data. Doesn't use
     * indexes and caches in segments.
     * 
     * This method should be closed at the end of usage. For example:
     * 
     * <pre>
     * try (final Stream&#60;Entry&#60;Integer, String&#62;&#62; stream = index.getStream()) {
     *     final List&#60;Entry&#60;Integer, String&#62;&#62; list = stream
     *             .collect(Collectors.toList());
     *     // some other code
     * }
     * 
     * </pre>
     * 
     * @param segmentWindows allows to limit examined segments. If empty then
     *                       all segments are used.
     * @return stream of all data. Equivalent to
     *         {@link #getStream(SegmentWindow, SegmentIteratorIsolation)} with
     *         {@link SegmentIteratorIsolation#FAIL_FAST}.
     */
    Stream<Entry<K, V>> getStream(SegmentWindow segmentWindows);

    /**
     * Streams entries using the requested iterator isolation level.
     *
     * @param segmentWindows segment selection to stream
     * @param isolation      iterator isolation mode to use
     * @return stream of key-value entries from the selected segments
     */
    default Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows,
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(isolation, "isolation");
        if (isolation == SegmentIteratorIsolation.FAIL_FAST) {
            return getStream(segmentWindows);
        }
        throw new UnsupportedOperationException(
                "FULL_ISOLATION streaming is not supported.");
    }

    /**
     * Convenience shortcut for streaming all segments.
     *
     * @return sequential stream of all entries
     */
    default Stream<Entry<K, V>> getStream() {
        return getStream(SegmentWindow.unbounded());
    }

    /**
     * Convenience shortcut for streaming all segments with explicit isolation.
     *
     * @param isolation iterator isolation mode to use
     * @return sequential stream of all entries
     */
    default Stream<Entry<K, V>> getStream(
            final SegmentIteratorIsolation isolation) {
        return getStream(SegmentWindow.unbounded(), isolation);
    }

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

    /**
     * Returns the configuration this index was opened with (merged with
     * defaults where appropriate).
     *
     * @return effective configuration
     */
    IndexConfiguration<K, V> getConfiguration();

    /**
     * Returns the current lifecycle state of this index instance.
     * Typical transitions are `OPENING -> READY -> CLOSING -> CLOSED`, with
     * `ERROR` as a terminal failure state.
     *
     * @return index state
     */
    SegmentIndexState getState();

    /**
     * Closes the index and releases owned resources.
     * Other threads may observe {@link SegmentIndexState#CLOSING} while close
     * is waiting for in-flight maintenance and durability boundaries to
     * settle.
     */
    @Override
    void close();

    /**
     * Returns an immutable snapshot of index-level operation counters and
     * lifecycle state.
     *
     * Implementations should override this method to expose non-zero counters.
     * The default keeps backward compatibility for custom implementations that
     * have not yet added metrics support.
     *
     * @return metrics snapshot
     */
    default SegmentIndexMetricsSnapshot metricsSnapshot() {
        return new SegmentIndexMetricsSnapshot(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0,
                0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0D, 0L, 0L, 0L, 0L, List.of(),
                getState());
    }

    /**
     * Returns runtime monitoring/tuning control-plane entry point.
     *
     * @return index control plane
     */
    default IndexControlPlane controlPlane() {
        throw new UnsupportedOperationException(
                "controlPlane() is not supported by this implementation.");
    }

}
