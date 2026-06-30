package org.hestiastore.index.segmentindex;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.monitoring.SegmentIndexRuntimeMonitoring;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexBootstrapOperation;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexRuntimeHandle;

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
     * The returned index does not own the supplied directory facade. Callers that
     * pass a closeable directory remain responsible for closing it.
     * </p>
     *
     * @param directory backing directory for the index
     * @param indexConf requested configuration overrides
     * @return a newly created index instance
     */
    static <M, N> SegmentIndex<M, N> create(final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        return operation(directory, indexConf, null).create();
    }

    /**
     * Creates a brand new index using caller-owned runtime resources.
     * <p>
     * The returned index does not own the supplied runtime. The caller remains
     * responsible for closing the runtime after all indexes using it have been
     * closed.
     * </p>
     *
     * @param directory backing directory for the index
     * @param indexConf requested configuration overrides
     * @param runtime caller-owned shared runtime
     * @return a newly created index instance
     */
    static <M, N> SegmentIndex<M, N> create(final Directory directory,
            final IndexConfiguration<M, N> indexConf,
            final HestiaStoreRuntime runtime) {
        return operation(directory, indexConf, null, runtime).create();
    }

    /**
     * Creates a brand new index using an explicit chunk filter provider
     * resolver.
     *
     * @param <M>                         key type
     * @param <N>                         value type
     * @param directory                   backing directory for the index
     * @param indexConf                   requested configuration overrides
     * @param chunkFilterProviderResolver resolver used to resolve persisted
     *                                    chunk filter specs; must not be null
     * @return a newly created index instance
     */
    static <M, N> SegmentIndex<M, N> create(final Directory directory,
            final IndexConfiguration<M, N> indexConf,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return operation(directory, indexConf,
                requireChunkFilterProviderResolver(
                        chunkFilterProviderResolver)).create();
    }

    /**
     * Opens an existing index, merging the provided configuration overrides
     * with the stored configuration on disk.
     * <p>
     * The returned index does not own the supplied directory facade. Callers that
     * pass a closeable directory remain responsible for closing it.
     * </p>
     *
     * @param directory backing directory that already contains the index
     * @param indexConf configuration overrides to apply
     * @return index instance backed by the updated configuration
     */
    static <M, N> SegmentIndex<M, N> open(final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        return operation(directory, indexConf, null).open();
    }

    /**
     * Opens an existing index using caller-owned runtime resources.
     * <p>
     * The returned index does not own the supplied runtime. The caller remains
     * responsible for closing the runtime after all indexes using it have been
     * closed.
     * </p>
     *
     * @param directory backing directory that already contains the index
     * @param indexConf configuration overrides to apply
     * @param runtime caller-owned shared runtime
     * @return index instance backed by the updated configuration
     */
    static <M, N> SegmentIndex<M, N> open(final Directory directory,
            final IndexConfiguration<M, N> indexConf,
            final HestiaStoreRuntime runtime) {
        return operation(directory, indexConf, null, runtime).open();
    }

    /**
     * Opens an existing index with an explicit chunk filter provider resolver.
     *
     * @param <M>                         key type
     * @param <N>                         value type
     * @param directory                   backing directory that already contains
     *                                    the index
     * @param indexConf                   configuration overrides to apply
     * @param chunkFilterProviderResolver resolver used to resolve persisted
     *                                    chunk filter specs; must not be null
     * @return index instance backed by the updated configuration
     */
    static <M, N> SegmentIndex<M, N> open(final Directory directory,
            final IndexConfiguration<M, N> indexConf,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return operation(directory, indexConf,
                requireChunkFilterProviderResolver(
                        chunkFilterProviderResolver)).open();
    }

    /**
     * Opens an existing index using the configuration stored on disk.
     * <p>
     * The returned index does not own the supplied directory facade. Callers that
     * pass a closeable directory remain responsible for closing it.
     * </p>
     *
     * @param directory backing directory with an existing index
     * @return index instance backed by the persisted configuration
     */
    static <M, N> SegmentIndex<M, N> open(final Directory directory) {
        return SegmentIndex.<M, N>operation(directory, emptyConfiguration(),
                null).open();
    }

    /**
     * Opens an existing index using stored configuration and caller-owned
     * runtime resources.
     *
     * @param directory backing directory with an existing index
     * @param runtime caller-owned shared runtime
     * @return index instance backed by the persisted configuration
     */
    static <M, N> SegmentIndex<M, N> open(final Directory directory,
            final HestiaStoreRuntime runtime) {
        return SegmentIndex.<M, N>operation(directory, emptyConfiguration(),
                null, runtime).open();
    }

    /**
     * Opens an existing index using stored configuration and an explicit chunk
     * filter provider resolver.
     *
     * @param <M>                         key type
     * @param <N>                         value type
     * @param directory                   backing directory with an existing
     *                                    index
     * @param chunkFilterProviderResolver resolver used to resolve persisted
     *                                    chunk filter specs; must not be null
     * @return index instance backed by the persisted configuration
     */
    static <M, N> SegmentIndex<M, N> open(final Directory directory,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return SegmentIndex.<M, N>operation(directory, emptyConfiguration(),
                requireChunkFilterProviderResolver(
                        chunkFilterProviderResolver)).open();
    }

    /**
     * Attempts to open an index when it may or may not exist.
     * <p>
     * The returned index does not own the supplied directory facade. Callers that
     * pass a closeable directory remain responsible for closing it.
     * </p>
     *
     * @param directory backing directory that may contain an index
     * @return optional index instance if the configuration was found
     */
    static <M, N> Optional<SegmentIndex<M, N>> tryOpen(
            final Directory directory) {
        return SegmentIndex.<M, N>operation(directory, emptyConfiguration(),
                null).tryOpen();
    }

    /**
     * Attempts to open an index using caller-owned runtime resources.
     *
     * @param directory backing directory that may contain an index
     * @param runtime caller-owned shared runtime
     * @return optional index instance if the configuration was found
     */
    static <M, N> Optional<SegmentIndex<M, N>> tryOpen(
            final Directory directory,
            final HestiaStoreRuntime runtime) {
        return SegmentIndex.<M, N>operation(directory, emptyConfiguration(),
                null, runtime).tryOpen();
    }

    /**
     * Attempts to open an index using an explicit chunk filter provider
     * resolver.
     *
     * @param <M>                         key type
     * @param <N>                         value type
     * @param directory                   backing directory that may contain an
     *                                    index
     * @param chunkFilterProviderResolver resolver used to resolve persisted
     *                                    chunk filter specs; must not be null
     * @return optional index instance if the configuration was found
     */
    static <M, N> Optional<SegmentIndex<M, N>> tryOpen(
            final Directory directory,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return SegmentIndex.<M, N>operation(directory, emptyConfiguration(),
                requireChunkFilterProviderResolver(
                        chunkFilterProviderResolver)).tryOpen();
    }

    private static <M, N> SegmentIndexBootstrapOperation<M, N> operation(
            final Directory directory,
            final IndexConfiguration<M, N> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return operation(
                requireDirectory(directory),
                requireUserProvidedConfiguration(userProvidedConfiguration),
                chunkFilterProviderResolver,
                HestiaStoreRuntimeAccess.owned());
    }

    private static <M, N> SegmentIndexBootstrapOperation<M, N> operation(
            final Directory directory,
            final IndexConfiguration<M, N> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver,
            final HestiaStoreRuntime runtime) {
        return operation(
                requireDirectory(directory),
                requireUserProvidedConfiguration(userProvidedConfiguration),
                chunkFilterProviderResolver,
                HestiaStoreRuntimeAccess.borrowed(runtime));
    }

    private static <M, N> SegmentIndexBootstrapOperation<M, N> operation(
            final Directory directory,
            final IndexConfiguration<M, N> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver,
            final SegmentIndexRuntimeHandle runtimeHandle) {
        return new SegmentIndexBootstrapOperation<>(
                requireDirectory(directory),
                requireUserProvidedConfiguration(userProvidedConfiguration),
                chunkFilterProviderResolver,
                Vldtn.requireNonNull(runtimeHandle, "runtimeHandle"));
    }

    private static Directory requireDirectory(final Directory directory) {
        return Vldtn.requireNonNull(directory, "directory");
    }

    private static <M, N> IndexConfiguration<M, N> requireUserProvidedConfiguration(
            final IndexConfiguration<M, N> userProvidedConfiguration) {
        return Vldtn.requireNonNull(userProvidedConfiguration,
                "userProvidedConfiguration");
    }

    private static ChunkFilterProviderResolver requireChunkFilterProviderResolver(
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return Vldtn.requireNonNull(chunkFilterProviderResolver,
                "chunkFilterProviderResolver");
    }

    private static <M, N> IndexConfiguration<M, N> emptyConfiguration() {
        return IndexConfiguration.<M, N>builder().build();
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
     * Closes the index and releases owned resources.
     * Other threads may observe {@link SegmentIndexState#CLOSING} while close
     * is waiting for in-flight maintenance and durability boundaries to
     * settle.
     */
    @Override
    void close();

    /**
     * Returns runtime monitoring view for this index.
     *
     * @return runtime monitoring view
     */
    SegmentIndexRuntimeMonitoring runtimeMonitoring();

    /**
     * Returns memory-estimation diagnostics captured during successful startup.
     * <p>
     * Implementations that do not capture startup diagnostics return an incomplete
     * report.
     * </p>
     *
     * @return startup memory-estimation report
     */
    default MemoryEstimateReport startupMemoryEstimate() {
        return new MemoryEstimateReport(
                List.of("startup memory estimate unavailable for this implementation"),
                false, OptionalLong.empty());
    }

    /**
     * Returns runtime tuning API.
     *
     * @return runtime tuning API
     */
    RuntimeTuning runtimeTuning();

    /**
     * Returns operational maintenance API.
     *
     * @return maintenance API
     */
    SegmentIndexMaintenance maintenance();

}
