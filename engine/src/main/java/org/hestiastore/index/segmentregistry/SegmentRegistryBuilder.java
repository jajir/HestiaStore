package org.hestiastore.index.segmentregistry;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;

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
    private IndexRuntimeConfiguration<K, V> runtimeConfiguration;
    private ExecutorService segmentMaintenanceExecutor;
    private ExecutorService registryMaintenanceExecutor;
    private SegmentIdAllocator segmentIdAllocator;

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
     * Sets the resolved runtime configuration used to materialize fresh chunk
     * filter pipelines for loaded segments.
     *
     * @param runtimeConfiguration resolved runtime configuration
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withRuntimeConfiguration(
            final IndexRuntimeConfiguration<K, V> runtimeConfiguration) {
        this.runtimeConfiguration = Vldtn.requireNonNull(runtimeConfiguration,
                "runtimeConfiguration");
        return this;
    }

    /**
     * Sets the segment maintenance executor.
     *
     * @param segmentMaintenanceExecutor segment maintenance executor
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withSegmentMaintenanceExecutor(
            final ExecutorService segmentMaintenanceExecutor) {
        this.segmentMaintenanceExecutor = Vldtn
                .requireNonNull(segmentMaintenanceExecutor,
                        "segmentMaintenanceExecutor");
        return this;
    }

    /**
     * Sets the registry maintenance executor used for load/unload operations.
     *
     * @param registryMaintenanceExecutor registry maintenance executor
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withRegistryMaintenanceExecutor(
            final ExecutorService registryMaintenanceExecutor) {
        this.registryMaintenanceExecutor = Vldtn
                .requireNonNull(registryMaintenanceExecutor,
                        "registryMaintenanceExecutor");
        return this;
    }

    /**
     * Sets the segment id allocator used for newly created segment ids.
     *
     * @param segmentIdAllocator allocator for segment ids
     * @return this builder
     */
    SegmentRegistryBuilder<K, V> withSegmentIdAllocator(
            final SegmentIdAllocator segmentIdAllocator) {
        this.segmentIdAllocator = Vldtn.requireNonNull(segmentIdAllocator,
                "segmentIdAllocator");
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
        final ExecutorService resolvedSegmentMaintenanceExecutor = Vldtn
                .requireNonNull(segmentMaintenanceExecutor,
                        "segmentMaintenanceExecutor");
        final ExecutorService resolvedRegistryMaintenanceExecutor = Vldtn
                .requireNonNull(registryMaintenanceExecutor,
                        "registryMaintenanceExecutor");
        final IndexRuntimeConfiguration<K, V> resolvedRuntimeConfiguration = runtimeConfiguration == null
                ? resolvedConf.resolveRuntimeConfiguration()
                : runtimeConfiguration;
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
                resolvedValueDescriptor, resolvedConf,
                resolvedRuntimeConfiguration,
                resolvedSegmentMaintenanceExecutor);
        final SegmentIdAllocator resolvedAllocator = segmentIdAllocator == null
                ? new DirectorySegmentIdAllocator(resolvedDirectory)
                : segmentIdAllocator;
        final SegmentRegistryFileSystem resolvedFileSystem = new SegmentRegistryFileSystem(
                resolvedDirectory);
        // SegmentIndex key-map bootstraps the first logical segment with id 0.
        // Ensure its directory exists so registry loads do not fail on a fresh index.
        resolvedFileSystem.ensureSegmentDirectory(SegmentId.of(0));
        final BusyRetryPolicy resolvedCloseRetryPolicy = new BusyRetryPolicy(
                busyBackoffMillis, busyTimeoutMillis);
        final BusyRetryPolicy resolvedBlockingRetryPolicy = new BusyRetryPolicy(
                busyBackoffMillis, busyTimeoutMillis);
        final BusyRetryPolicy resolvedRegistryCloseRetryPolicy = new BusyRetryPolicy(
                busyBackoffMillis, REGISTRY_CLOSE_TIMEOUT_MILLIS);
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();
        final Set<SegmentId> pinnedSegments = ConcurrentHashMap.newKeySet();
        final SegmentLifecycleMaintenance<K, V> maintenance = new SegmentLifecycleMaintenance<>(
                resolvedFactory, resolvedFileSystem, resolvedCloseRetryPolicy,
                gate);
        final SegmentRegistryCache<SegmentId, Segment<K, V>> cache = new SegmentRegistryCache<>(
                maxNumberOfSegmentsInCache, maintenance::loadSegment,
                maintenance::closeSegmentIfNeeded,
                resolvedRegistryMaintenanceExecutor,
                segment -> isEvictable(segment, pinnedSegments, gate));
        return new SegmentRegistryImpl<>(resolvedAllocator, resolvedFileSystem,
                cache, resolvedRegistryCloseRetryPolicy, gate, resolvedFactory,
                resolvedFactory, resolvedBlockingRetryPolicy, pinnedSegments);
    }

    private static <K, V> boolean isEvictable(final Segment<K, V> segment,
            final Set<SegmentId> pinnedSegments,
            final SegmentRegistryStateMachine gate) {
        return segment != null && (segment.getState() == SegmentState.CLOSED
                || isUnpinnedReadySegment(segment, pinnedSegments, gate));
    }

    private static <K, V> boolean isUnpinnedReadySegment(
            final Segment<K, V> segment, final Set<SegmentId> pinnedSegments,
            final SegmentRegistryStateMachine gate) {
        final boolean closing = gate.getState() != SegmentRegistryState.READY;
        final SegmentId segmentId = segment.getId();
        return segmentId != null && !pinnedSegments.contains(segmentId)
                && segment.getState() == SegmentState.READY
                && (closing || segment.getNumberOfKeysInWriteCache() == 0);
    }

    private static int sanitizeRetryConf(final Integer configured,
            final int fallback) {
        if (configured == null || configured.intValue() < 1) {
            return fallback;
        }
        return configured.intValue();
    }
}
