package org.hestiastore.index.sst;

import java.util.Optional;
import java.util.stream.Stream;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Pair;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.log.Log;
import org.hestiastore.index.log.LoggedKey;
import org.hestiastore.index.unsorteddatafile.UnsortedDataFileStreamer;

public interface Index<K, V> extends CloseableResource {

    static <M, N> Index<M, N> create(final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        final IndexConfigurationManager<M, N> confManager = new IndexConfigurationManager<>(
                new IndexConfiguratonStorage<>(directory));
        final IndexConfiguration<M, N> conf = confManager
                .applyDefaults(indexConf);
        confManager.save(conf);
        return openIndex(directory, conf);
    }

    static <M, N> Index<M, N> open(final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        final IndexConfigurationManager<M, N> confManager = new IndexConfigurationManager<>(
                new IndexConfiguratonStorage<>(directory));
        final IndexConfiguration<M, N> mergedConf = confManager
                .mergeWithStored(indexConf);
        return openIndex(directory, mergedConf);
    }

    static <M, N> Index<M, N> open(final Directory directory) {
        final IndexConfigurationManager<M, N> confManager = new IndexConfigurationManager<>(
                new IndexConfiguratonStorage<>(directory));
        return openIndex(directory, confManager.loadExisting());
    }

    static <M, N> Optional<Index<M, N>> tryOpen(final Directory directory) {
        final IndexConfigurationManager<M, N> confManager = new IndexConfigurationManager<>(
                new IndexConfiguratonStorage<>(directory));
        final Optional<IndexConfiguration<M, N>> oConf = confManager
                .tryToLoad();
        if (oConf.isPresent()) {
            return Optional.of(openIndex(directory, oConf.get()));
        } else {
            return Optional.empty();
        }
    }

    private static <M, N> Index<M, N> openIndex(final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        Log<M, N> log = null;
        final TypeDescriptor<M> keyTypeDescriptor = DataTypeDescriptorRegistry
                .makeInstance(indexConf.getKeyTypeDescriptor());
        final TypeDescriptor<N> valueTypeDescriptor = DataTypeDescriptorRegistry
                .makeInstance(indexConf.getValueTypeDescriptor());
        Vldtn.requireNonNull(indexConf.isLogEnabled(), "isLogEnabled");
        if (Boolean.TRUE.equals(indexConf.isLogEnabled())) {
            log = Log.<M, N>builder()//
                    .withDirectory(directory)//
                    .withKeyTypeDescriptor(keyTypeDescriptor)//
                    .withValueTypeDescriptor(valueTypeDescriptor)//
                    .build();
        } else {
            log = Log.<M, N>builder().buildEmpty();
        }
        Vldtn.requireNonNull(indexConf.isThreadSafe(), "isThreadSafe");
        if (Boolean.TRUE.equals(indexConf.isThreadSafe())) {
            final IndexInternal<M, N> index = new IndexInternalSynchronized<>(
                    directory, keyTypeDescriptor, valueTypeDescriptor,
                    indexConf, log);
            return new IndexContextLoggingAdapter<>(indexConf, index);
        } else {
            final IndexInternal<M, N> index = new IndexInternalDefault<>(
                    directory, keyTypeDescriptor, valueTypeDescriptor,
                    indexConf, log);
            return new IndexContextLoggingAdapter<>(indexConf, index);
        }
    }

    void put(K key, V value);

    default void put(final Pair<K, V> pair) {
        Vldtn.requireNonNull(pair, "pair");
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
     * This method should be closed at the end of usage. For example:
     * 
     * <pre>
     * try (final Stream&#60;Pair&#60;Integer, String&#62;&#62; stream = index.getStream()) {
     *     final List&#60;Pair&#60;Integer, String&#62;&#62; list = stream
     *             .collect(Collectors.toList());
     *     // some other code
     * }
     * 
     * </pre>
     * 
     * @param segmentWindows allows to limit examined segments. If empty then
     *                       all segments are used.
     * @return stream of all data.
     */
    Stream<Pair<K, V>> getStream(SegmentWindow segmentWindows);

    default Stream<Pair<K, V>> getStream() {
        return getStream(SegmentWindow.unbounded());
    }

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

    /**
     * Returns the configuration of the index.
     *
     * @return the configuration of the index
     */
    IndexConfiguration<K, V> getConfiguration();
}
