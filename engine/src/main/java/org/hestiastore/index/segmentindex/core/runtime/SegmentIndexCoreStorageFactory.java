package org.hestiastore.index.segmentindex.core.runtime;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Opens the core storage collaborators needed before split and runtime service
 * assembly can proceed.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexCoreStorageFactory<K, V> {

    private final SegmentIndexRuntimeInputs<K, V> request;
    private final SegmentIndexRuntimeGraphBuilder.ResourceCreationObserver<K, V> resourceCreationObserver;

    SegmentIndexCoreStorageFactory(
            final SegmentIndexRuntimeInputs<K, V> request,
            final SegmentIndexRuntimeGraphBuilder.ResourceCreationObserver<K, V> resourceCreationObserver) {
        this.request = Vldtn.requireNonNull(request, "request");
        this.resourceCreationObserver = Vldtn.requireNonNull(
                resourceCreationObserver, "resourceCreationObserver");
    }

    SegmentIndexCoreStorage<K, V> create() {
        final RuntimeTuningState runtimeTuningState = RuntimeTuningState
                .fromConfiguration(request.conf);
        final KeyToSegmentMapImpl<K> keyToSegmentMapDelegate = new KeyToSegmentMapImpl<>(
                request.directoryFacade, request.keyTypeDescriptor);
        final KeyToSegmentMap<K> keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMapDelegate);
        resourceCreationObserver.onKeyToSegmentMapCreated(keyToSegmentMap);
        final SegmentRegistry<K, V> segmentRegistry = newSegmentRegistry();
        resourceCreationObserver.onSegmentRegistryCreated(segmentRegistry);
        return new SegmentIndexCoreStorage<>(runtimeTuningState, keyToSegmentMap,
                segmentRegistry,
                newRetryPolicy(request.conf));
    }

    private SegmentRegistry<K, V> newSegmentRegistry() {
        return SegmentRegistry.<K, V>builder()
                .withDirectoryFacade(request.directoryFacade)
                .withKeyTypeDescriptor(request.keyTypeDescriptor)
                .withValueTypeDescriptor(request.valueTypeDescriptor)
                .withConfiguration(request.conf)
                .withRuntimeConfiguration(request.runtimeConfiguration)
                .withSegmentMaintenanceExecutor(
                        request.executorRegistry
                                .getStableSegmentMaintenanceExecutor())
                .withRegistryMaintenanceExecutor(
                        request.executorRegistry.getRegistryMaintenanceExecutor())
                .build();
    }

    private IndexRetryPolicy newRetryPolicy(
            final IndexConfiguration<K, V> conf) {
        return new IndexRetryPolicy(conf.getIndexBusyBackoffMillis(),
                conf.getIndexBusyTimeoutMillis());
    }
}
