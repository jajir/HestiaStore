package org.hestiastore.index.segmentindex;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.SegmentIteratorIsolation;

/**
 * High-level contract for the segment-index layer that sits above individual
 * segments. It supports creating/opening instances, point mutations
 * (put/delete), range streaming, log inspection, and consistency checks.
 * Concrete implementations are created through the static factory helpers which
 * take care of configuration handling and async directory wrapping.
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
     * The supplied directory is wrapped in an {@link AsyncDirectoryAdapter}
     * configured with the resolved {@code numberOfIoThreads}. The returned
     * index owns that wrapper and will close it when the index is closed.
     * </p>
     *
     * @param directory backing directory for the index
     * @param indexConf requested configuration overrides
     * @return a newly created index instance
     */
    static <M, N> SegmentIndex<M, N> create(
            final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        final int initialIoThreads = resolveIoThreads(
                indexConf.getNumberOfIoThreads());
        AsyncDirectory asyncDirectory = AsyncDirectoryAdapter.wrap(directory,
                initialIoThreads);
        try {
            IndexConfigurationManager<M, N> confManager = new IndexConfigurationManager<>(
                    new IndexConfiguratonStorage<>(asyncDirectory));
            final IndexConfiguration<M, N> conf = confManager
                    .applyDefaults(indexConf);
            final int configuredIoThreads = resolveIoThreads(
                    conf.getNumberOfIoThreads());
            if (configuredIoThreads != initialIoThreads) {
                asyncDirectory.close();
                asyncDirectory = AsyncDirectoryAdapter.wrap(directory,
                        configuredIoThreads);
                confManager = new IndexConfigurationManager<>(
                        new IndexConfiguratonStorage<>(asyncDirectory));
            }
            confManager.save(conf);
            return openIndex(asyncDirectory, conf);
        } catch (final RuntimeException e) {
            closeOnFailure(asyncDirectory, e);
            throw e;
        }
    }

    /**
     * Opens an existing index, merging the provided configuration overrides
     * with the stored configuration on disk.
     * <p>
     * The supplied directory is wrapped in an {@link AsyncDirectoryAdapter}
     * configured with the merged {@code numberOfIoThreads}. The returned index
     * owns that wrapper and will close it when the index is closed.
     * </p>
     *
     * @param directory backing directory that already contains the index
     * @param indexConf configuration overrides to apply
     * @return index instance backed by the updated configuration
     */
    static <M, N> SegmentIndex<M, N> open(
            final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        final int initialIoThreads = resolveIoThreads(
                indexConf.getNumberOfIoThreads());
        AsyncDirectory asyncDirectory = AsyncDirectoryAdapter.wrap(directory,
                initialIoThreads);
        try {
            final IndexConfigurationManager<M, N> confManager = new IndexConfigurationManager<>(
                    new IndexConfiguratonStorage<>(asyncDirectory));
            final IndexConfiguration<M, N> mergedConf = confManager
                    .mergeWithStored(indexConf);
            final int configuredIoThreads = resolveIoThreads(
                    mergedConf.getNumberOfIoThreads());
            if (configuredIoThreads != initialIoThreads) {
                asyncDirectory.close();
                asyncDirectory = AsyncDirectoryAdapter.wrap(directory,
                        configuredIoThreads);
            }
            return openIndex(asyncDirectory, mergedConf);
        } catch (final RuntimeException e) {
            closeOnFailure(asyncDirectory, e);
            throw e;
        }
    }

    /**
     * Opens an existing index using the configuration stored on disk.
     * <p>
     * The supplied directory is wrapped in an {@link AsyncDirectoryAdapter}
     * configured with the stored {@code numberOfIoThreads}. The returned index
     * owns that wrapper and will close it when the index is closed.
     * </p>
     *
     * @param directory backing directory with an existing index
     * @return index instance backed by the persisted configuration
     */
    static <M, N> SegmentIndex<M, N> open(
            final Directory directory) {
        AsyncDirectory asyncDirectory = AsyncDirectoryAdapter.wrap(directory,
                IndexConfigurationContract.NUMBER_OF_IO_THREADS);
        try {
            final IndexConfigurationManager<M, N> confManager = new IndexConfigurationManager<>(
                    new IndexConfiguratonStorage<>(asyncDirectory));
            final IndexConfiguration<M, N> conf = confManager.loadExisting();
            final int configuredIoThreads = resolveIoThreads(
                    conf.getNumberOfIoThreads());
            if (configuredIoThreads
                    != IndexConfigurationContract.NUMBER_OF_IO_THREADS) {
                asyncDirectory.close();
                asyncDirectory = AsyncDirectoryAdapter.wrap(directory,
                        configuredIoThreads);
            }
            return openIndex(asyncDirectory, conf);
        } catch (final RuntimeException e) {
            closeOnFailure(asyncDirectory, e);
            throw e;
        }
    }

    /**
     * Attempts to open an index when it may or may not exist.
     * <p>
     * The supplied directory is wrapped in an {@link AsyncDirectoryAdapter}
     * configured with the stored {@code numberOfIoThreads} when a configuration
     * is present. The returned index owns that wrapper and will close it when
     * the index is closed.
     * </p>
     *
     * @param directory backing directory that may contain an index
     * @return optional index instance if the configuration was found
     */
    static <M, N> Optional<SegmentIndex<M, N>> tryOpen(
            final Directory directory) {
        AsyncDirectory asyncDirectory = AsyncDirectoryAdapter.wrap(directory,
                IndexConfigurationContract.NUMBER_OF_IO_THREADS);
        try {
            final IndexConfigurationManager<M, N> confManager = new IndexConfigurationManager<>(
                    new IndexConfiguratonStorage<>(asyncDirectory));
            final Optional<IndexConfiguration<M, N>> oConf = confManager
                    .tryToLoad();
            if (oConf.isEmpty()) {
                asyncDirectory.close();
                return Optional.empty();
            }
            final IndexConfiguration<M, N> conf = oConf.get();
            final int configuredIoThreads = resolveIoThreads(
                    conf.getNumberOfIoThreads());
            if (configuredIoThreads
                    != IndexConfigurationContract.NUMBER_OF_IO_THREADS) {
                asyncDirectory.close();
                asyncDirectory = AsyncDirectoryAdapter.wrap(directory,
                        configuredIoThreads);
            }
            return Optional.of(openIndex(asyncDirectory, conf));
        } catch (final RuntimeException e) {
            closeOnFailure(asyncDirectory, e);
            throw e;
        }
    }

    private static <M, N> SegmentIndex<M, N> openIndex(
            final AsyncDirectory directoryFacade,
            final IndexConfiguration<M, N> indexConf) {
        final TypeDescriptor<M> keyTypeDescriptor = DataTypeDescriptorRegistry
                .makeInstance(indexConf.getKeyTypeDescriptor());
        final TypeDescriptor<N> valueTypeDescriptor = DataTypeDescriptorRegistry
                .makeInstance(indexConf.getValueTypeDescriptor());
        Vldtn.requireNonNull(indexConf.isContextLoggingEnabled(),
                "isContextLoggingEnabled");
        SegmentIndex<M, N> index = new IndexInternalConcurrent<>(
                directoryFacade, keyTypeDescriptor, valueTypeDescriptor,
                indexConf);
        if (Boolean.TRUE.equals(indexConf.isContextLoggingEnabled())) {
            index = new IndexContextLoggingAdapter<>(indexConf, index);
        }
        index = new IndexAsyncAdapter<>(index);
        return new IndexDirectoryClosingAdapter<>(index, directoryFacade);
    }

    private static int resolveIoThreads(final Integer configuredThreads) {
        if (configuredThreads == null || configuredThreads.intValue() < 1) {
            return IndexConfigurationContract.NUMBER_OF_IO_THREADS;
        }
        return configuredThreads.intValue();
    }

    private static void closeOnFailure(final AsyncDirectory asyncDirectory,
            final RuntimeException failure) {
        if (asyncDirectory == null) {
            return;
        }
        try {
            asyncDirectory.close();
        } catch (final RuntimeException closeError) {
            failure.addSuppressed(closeError);
        }
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
     * Asynchronously inserts or updates a single entry.
     *
     * @param key   key to write (must not be null)
     * @param value value to associate with the key (must not be null and must
     *              not be a tombstone value)
     * @return completion that finishes when the write completes
     */
    CompletionStage<Void> putAsync(K key, V value);

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
     * Performs an asynchronous point lookup for the given key.
     *
     * @param key key to search for
     * @return completion that supplies the stored value or {@code null} when no
     *         entry exists
     */
    CompletionStage<V> getAsync(K key);

    /**
     * Deletes (tombstones) the provided key.
     *
     * @param key key to remove from the index
     */
    void delete(K key);

    /**
     * Asynchronously tombstones the provided key.
     *
     * @param key key to remove from the index
     * @return completion that finishes when the delete completes
     */
    CompletionStage<Void> deleteAsync(K key);

    /**
     * Starts a compaction pass over in-memory and on-disk data structures.
     * This is the explicit index-level entry point for compaction; automatic
     * split logic does not invoke compaction.
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
     * @return stream of all data.
     * Equivalent to
     * {@link #getStream(SegmentWindow, SegmentIteratorIsolation)} with
     * {@link SegmentIteratorIsolation#FAIL_FAST}.
     */
    Stream<Entry<K, V>> getStream(SegmentWindow segmentWindows);

    /**
     * Streams entries using the requested iterator isolation level.
     *
     * @param segmentWindows segment selection to stream
     * @param isolation iterator isolation mode to use
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
     *
     * @return index state
     */
    SegmentIndexState getState();
}
