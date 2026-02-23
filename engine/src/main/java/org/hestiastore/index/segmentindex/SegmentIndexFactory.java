package org.hestiastore.index.segmentindex;

import java.util.Optional;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;

/**
 * Central lifecycle owner for SegmentIndex construction and dependent
 * resources.
 * <p>
 * This class owns the full startup/rollback flow for index create/open paths
 * and wires the managed close sequence used by returned index instances.
 * </p>
 */
final class SegmentIndexFactory {

    private SegmentIndexFactory() {
    }

    /**
     * Creates a new index, persists resolved configuration, and opens runtime
     * services.
     *
     * @param <M>       key type
     * @param <N>       value type
     * @param directory target index directory
     * @param indexConf user configuration overrides
     * @return opened index instance
     */
    static <M, N> SegmentIndex<M, N> create(final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        final SegmentIndexLifecycle<M, N> lifecycle = new SegmentIndexLifecycle<>(
                directory, indexConf);
        lifecycle.open(true);
        return openIndex(lifecycle.getIndexConfiguration(), lifecycle);
    }

    /**
     * Opens an existing index by merging stored and user configuration.
     *
     * @param <M>       key type
     * @param <N>       value type
     * @param directory target index directory
     * @param indexConf user configuration overrides
     * @return opened index instance
     */
    static <M, N> SegmentIndex<M, N> open(final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        final SegmentIndexLifecycle<M, N> lifecycle = new SegmentIndexLifecycle<>(
                directory, indexConf);
        lifecycle.open(false);
        return openIndex(lifecycle.getIndexConfiguration(), lifecycle);
    }

    /**
     * Opens an existing index using configuration stored on disk.
     *
     * @param <M>       key type
     * @param <N>       value type
     * @param directory target index directory
     * @return opened index instance
     */
    static <M, N> SegmentIndex<M, N> open(final Directory directory) {
        final IndexConfigurationManager<M, N> confManager = new IndexConfigurationManager<>(
                new IndexConfiguratonStorage<>(directory));
        final IndexConfiguration<M, N> conf = confManager.loadExisting();
        final SegmentIndexLifecycle<M, N> lifecycle = new SegmentIndexLifecycle<>(
                directory, conf);
        lifecycle.open(false);
        return openIndex(lifecycle.getIndexConfiguration(), lifecycle);
    }

    /**
     * Tries to open an index if configuration already exists in the directory.
     *
     * @param <M>       key type
     * @param <N>       value type
     * @param directory target index directory
     * @return optional opened index
     */
    static <M, N> Optional<SegmentIndex<M, N>> tryOpen(
            final Directory directory) {
        final IndexConfigurationManager<M, N> confManager = new IndexConfigurationManager<>(
                new IndexConfiguratonStorage<>(directory));
        final Optional<IndexConfiguration<M, N>> oConf = confManager
                .tryToLoad();
        if (oConf.isEmpty()) {
            return Optional.empty();
        }
        final SegmentIndexLifecycle<M, N> lifecycle = new SegmentIndexLifecycle<>(
                directory, oConf.get());
        lifecycle.open(false);
        return Optional
                .of(openIndex(lifecycle.getIndexConfiguration(), lifecycle));
    }

    /**
     * Builds the runtime index stack from resolved configuration and lifecycle
     * resources.
     *
     * @param <M>       key type
     * @param <N>       value type
     * @param indexConf resolved configuration
     * @param lifecycle lifecycle owning runtime dependencies
     * @return index wrapped with async and close adapters
     */
    private static <M, N> SegmentIndex<M, N> openIndex(
            final IndexConfiguration<M, N> indexConf,
            final SegmentIndexLifecycle<M, N> lifecycle) {
        final Directory directoryFacade = lifecycle.getManagedDirectory();
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
        final CloseableResource lifecycleOnClose = new AbstractCloseableResource() {
            @Override
            protected void doClose() {
                lifecycle.close();
            }
        };
        return new IndexDirectoryClosingAdapter<>(index, directoryFacade,
                lifecycleOnClose);
    }

}
