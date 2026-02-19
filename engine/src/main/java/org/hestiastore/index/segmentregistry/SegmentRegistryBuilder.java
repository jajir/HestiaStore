package org.hestiastore.index.segmentregistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;

/**
 * Builder for {@link SegmentRegistry} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentRegistryBuilder<K, V> {

    private static final int REGISTRY_CLOSE_TIMEOUT_MILLIS = (int) TimeUnit.MINUTES
            .toMillis(5);

    private Directory directoryFacade;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private IndexConfiguration<K, V> conf;
    private ExecutorService maintenanceExecutor;
    private ExecutorService lifecycleExecutor;
    private Runnable compactionRequestListener = () -> {};

    SegmentRegistryBuilder() {
    }

    /**
     * Sets the base directory for segments.
     *
     * @param directoryFacade base directory
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withDirectoryFacade(
            final Directory directoryFacade) {
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
     * Sets the registry lifecycle executor used for load/unload operations.
     *
     * @param lifecycleExecutor lifecycle executor
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withLifecycleExecutor(
            final ExecutorService lifecycleExecutor) {
        this.lifecycleExecutor = Vldtn.requireNonNull(lifecycleExecutor,
                "lifecycleExecutor");
        return this;
    }

    /**
     * Sets callback invoked when a segment accepts a compaction request.
     *
     * @param listener compaction callback
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withCompactionRequestListener(
            final Runnable listener) {
        this.compactionRequestListener = Vldtn.requireNonNull(listener,
                "compactionRequestListener");
        return this;
    }

    /**
     * Builds a registry with the configured defaults and overrides.
     *
     * @return registry instance
     */
    public SegmentRegistry<K, V> build() {
        final Directory resolvedDirectory = Vldtn.requireNonNull(
                directoryFacade, "directoryFacade");
        final TypeDescriptor<K> resolvedKeyDescriptor = Vldtn.requireNonNull(
                keyTypeDescriptor, "keyTypeDescriptor");
        final TypeDescriptor<V> resolvedValueDescriptor = Vldtn.requireNonNull(
                valueTypeDescriptor, "valueTypeDescriptor");
        final IndexConfiguration<K, V> resolvedConf = Vldtn.requireNonNull(conf,
                "conf");
        final ExecutorService resolvedExecutor = Vldtn.requireNonNull(
                maintenanceExecutor, "maintenanceExecutor");
        final ExecutorService resolvedLifecycleExecutor = Vldtn.requireNonNull(
                lifecycleExecutor, "lifecycleExecutor");
        final int maxSegments = Vldtn
                .requireNonNull(resolvedConf.getMaxNumberOfSegmentsInCache(),
                        "maxNumberOfSegmentsInCache")
                .intValue();
        final int maxNumberOfSegmentsInCache = Vldtn.requireGreaterThanZero(
                maxSegments, "maxNumberOfSegmentsInCache");
        final int busyBackoffMillis = sanitizeRetryConf(
                resolvedConf.getIndexBusyBackoffMillis(),
                IndexConfigurationContract.DEFAULT_INDEX_BUSY_BACKOFF_MILLIS);
        final int busyTimeoutMillis = sanitizeRetryConf(
                resolvedConf.getIndexBusyTimeoutMillis(),
                IndexConfigurationContract.DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS);
        final SegmentFactory<K, V> resolvedFactory = new SegmentFactory<>(
                resolvedDirectory, resolvedKeyDescriptor,
                resolvedValueDescriptor, resolvedConf, resolvedExecutor,
                compactionRequestListener);
        final SegmentIdAllocator resolvedAllocator = new DirectorySegmentIdAllocator(
                resolvedDirectory);
        final SegmentRegistryFileSystem resolvedFileSystem = new SegmentRegistryFileSystem(
                resolvedDirectory);
        // SegmentIndex key-map bootstraps the first logical segment with id 0.
        // Ensure its directory exists so registry loads do not fail on a fresh index.
        resolvedFileSystem.ensureSegmentDirectory(SegmentId.of(0));
        final IndexRetryPolicy resolvedCloseRetryPolicy = new IndexRetryPolicy(
                busyBackoffMillis, busyTimeoutMillis);
        final IndexRetryPolicy resolvedRegistryCloseRetryPolicy = new IndexRetryPolicy(
                busyBackoffMillis, REGISTRY_CLOSE_TIMEOUT_MILLIS);
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();
        final SegmentLifecycleMaintenance<K, V> maintenance = new SegmentLifecycleMaintenance<>(
                resolvedFactory, resolvedFileSystem, resolvedCloseRetryPolicy,
                gate);
        final SegmentRegistryCache<SegmentId, Segment<K, V>> cache = new SegmentRegistryCache<>(
                maxNumberOfSegmentsInCache, maintenance::loadSegment,
                maintenance::closeSegmentIfNeeded, resolvedLifecycleExecutor,
                segment -> segment != null);
        return new SegmentRegistryImpl<>(resolvedAllocator, resolvedFileSystem,
                cache, resolvedRegistryCloseRetryPolicy, gate);
    }

    private static int sanitizeRetryConf(final Integer configured,
            final int fallback) {
        if (configured == null || configured.intValue() < 1) {
            return fallback;
        }
        return configured.intValue();
    }
}
