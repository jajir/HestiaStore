package org.hestiastore.index.segmentregistry;

import java.util.concurrent.ExecutorService;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;

/**
 * Builder for {@link SegmentRegistry} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentRegistryBuilder<K, V> {

    private AsyncDirectory directoryFacade;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private IndexConfiguration<K, V> conf;
    private ExecutorService maintenanceExecutor;

    private SegmentIdAllocator segmentIdAllocator;
    private SegmentFactory<K, V> segmentFactory;

    SegmentRegistryBuilder() {
    }

    /**
     * Sets the base directory for segments.
     *
     * @param directoryFacade base directory
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withDirectoryFacade(
            final AsyncDirectory directoryFacade) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        return this;
    }

    /**
     * Sets the key type descriptor.
     *
     * @param keyTypeDescriptor key type descriptor
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        return this;
    }

    /**
     * Sets the value type descriptor.
     *
     * @param valueTypeDescriptor value type descriptor
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        return this;
    }

    /**
     * Sets the index configuration.
     *
     * @param conf index configuration
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withConfiguration(
            final IndexConfiguration<K, V> conf) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        return this;
    }

    /**
     * Sets the maintenance executor.
     *
     * @param maintenanceExecutor maintenance executor
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withMaintenanceExecutor(
            final ExecutorService maintenanceExecutor) {
        this.maintenanceExecutor = Vldtn.requireNonNull(maintenanceExecutor,
                "maintenanceExecutor");
        return this;
    }

    /**
     * Overrides the default segment id allocator.
     *
     * @param segmentIdAllocator allocator instance
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withSegmentIdAllocator(
            final SegmentIdAllocator segmentIdAllocator) {
        this.segmentIdAllocator = Vldtn.requireNonNull(segmentIdAllocator,
                "segmentIdAllocator");
        return this;
    }

    /**
     * Overrides the default segment factory.
     *
     * @param segmentFactory segment factory
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withSegmentFactory(
            final SegmentFactory<K, V> segmentFactory) {
        this.segmentFactory = Vldtn.requireNonNull(segmentFactory,
                "segmentFactory");
        return this;
    }

    /**
     * Builds a registry with the configured defaults and overrides.
     *
     * @return registry instance
     */
    public SegmentRegistryImpl<K, V> build() {
        final AsyncDirectory resolvedDirectory = Vldtn.requireNonNull(
                directoryFacade, "directoryFacade");
        final TypeDescriptor<K> resolvedKeyDescriptor = Vldtn.requireNonNull(
                keyTypeDescriptor, "keyTypeDescriptor");
        final TypeDescriptor<V> resolvedValueDescriptor = Vldtn.requireNonNull(
                valueTypeDescriptor, "valueTypeDescriptor");
        final IndexConfiguration<K, V> resolvedConf = Vldtn.requireNonNull(conf,
                "conf");
        final ExecutorService resolvedExecutor = Vldtn.requireNonNull(
                maintenanceExecutor, "maintenanceExecutor");
        final SegmentFactory<K, V> resolvedFactory = segmentFactory != null
                ? segmentFactory
                : new SegmentFactory<>(resolvedDirectory,
                        resolvedKeyDescriptor, resolvedValueDescriptor,
                        resolvedConf, resolvedExecutor);
        final SegmentIdAllocator resolvedAllocator = segmentIdAllocator != null
                ? segmentIdAllocator
                : new DirectorySegmentIdAllocator(resolvedDirectory);
        return new SegmentRegistryImpl<>(resolvedDirectory, resolvedFactory,
                resolvedAllocator, resolvedConf);
    }
}
