package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorageFactory;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Builds the runtime collaborator graph used by {@link SegmentIndexRuntime}.
 *
 * @param <K> key type
 * @param <V> value type
 */
@SuppressWarnings({ "java:S6206", "java:S6539" })
public final class SegmentIndexRuntimeGraphBuilder<K, V> {

    /**
     * Observer notified as selected runtime resources are created during graph
     * assembly.
     *
     * @param <K> key type
     * @param <V> value type
     */
    public interface ResourceCreationObserver<K, V> {

        /**
         * Called after the route map was opened.
         *
         * @param keyToSegmentMap created route map
         */
        default void onKeyToSegmentMapCreated(
                final KeyToSegmentMap<K> keyToSegmentMap) {
        }

        /**
         * Called after the segment registry was opened.
         *
         * @param segmentRegistry created segment registry
         */
        default void onSegmentRegistryCreated(
                final SegmentRegistry<K, V> segmentRegistry) {
        }

        /**
         * Called after the WAL runtime was opened.
         *
         * @param walRuntime created WAL runtime
         */
        default void onWalRuntimeCreated(final WalRuntime<K, V> walRuntime) {
        }
    }

    private final SegmentIndexRuntimeInputs<K, V> request;
    private final ResourceCreationObserver<K, V> resourceCreationObserver;

    SegmentIndexRuntimeGraphBuilder(
            final SegmentIndexRuntimeInputs<K, V> request) {
        this(request, new ResourceCreationObserver<>() {
        });
    }

    SegmentIndexRuntimeGraphBuilder(
            final SegmentIndexRuntimeInputs<K, V> request,
            final ResourceCreationObserver<K, V> resourceCreationObserver) {
        this.request = Vldtn.requireNonNull(request, "request");
        this.resourceCreationObserver = Vldtn.requireNonNull(
                resourceCreationObserver, "resourceCreationObserver");
    }

    SegmentIndexRuntime<K, V> build() {
        KeyToSegmentMap<K> keyToSegmentMap = null;
        SegmentRegistry<K, V> segmentRegistry = null;
        WalRuntime<K, V> walRuntime = null;
        try {
            final SegmentIndexCoreStorage<K, V> coreStorage = openCoreStorage();
            keyToSegmentMap = coreStorage.keyToSegmentMap();
            segmentRegistry = coreStorage.segmentRegistry();
            final SegmentTopologyRuntime<K, V> topologyRuntime =
                    createTopologyRuntime(
                    coreStorage);
            walRuntime = openWalRuntime();
            notifyWalRuntimeCreated(walRuntime);
            return createRuntime(coreStorage, topologyRuntime, walRuntime);
        } catch (final RuntimeException failure) {
            throw cleanupFailedBuild(failure, walRuntime, segmentRegistry,
                    keyToSegmentMap);
        }
    }

    private SegmentIndexCoreStorage<K, V> openCoreStorage() {
        return new SegmentIndexCoreStorageFactory<>(request,
                resourceCreationObserver).create();
    }

    private SegmentTopologyRuntime<K, V> createTopologyRuntime(
            final SegmentIndexCoreStorage<K, V> coreStorage) {
        return new SegmentTopologyRuntime<>(request, coreStorage);
    }

    private SegmentIndexRuntime<K, V> createRuntime(
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentTopologyRuntime<K, V> topologyRuntime,
            final WalRuntime<K, V> walRuntime) {
        final SegmentIndexRuntimeServices<K, V> serviceState =
                new SegmentIndexRuntimeServicesFactory<>(request, coreStorage,
                        topologyRuntime).create(walRuntime);
        return new SegmentIndexRuntime<>(request.keyTypeDescriptor, coreStorage,
                topologyRuntime, walRuntime, serviceState);
    }

    private WalRuntime<K, V> openWalRuntime() {
        return WalRuntime.open(request.directoryFacade, request.conf.getWal(),
                request.keyTypeDescriptor, request.valueTypeDescriptor);
    }

    private void notifyWalRuntimeCreated(final WalRuntime<K, V> walRuntime) {
        try {
            resourceCreationObserver.onWalRuntimeCreated(walRuntime);
        } catch (final RuntimeException failure) {
            cleanupFailedBuild(failure, walRuntime, null, null);
            throw failure;
        }
    }

    private RuntimeException cleanupFailedBuild(
            final RuntimeException failure,
            final WalRuntime<K, V> walRuntime,
            final SegmentRegistry<K, V> segmentRegistry,
            final KeyToSegmentMap<K> keyToSegmentMap) {
        closeWalRuntime(walRuntime, failure);
        closeSegmentRegistry(segmentRegistry, failure);
        closeKeyToSegmentMap(keyToSegmentMap, failure);
        return failure;
    }

    private void closeWalRuntime(final WalRuntime<K, V> walRuntime,
            final RuntimeException failure) {
        if (walRuntime == null) {
            return;
        }
        try {
            walRuntime.close();
        } catch (final RuntimeException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private void closeSegmentRegistry(final SegmentRegistry<K, V> segmentRegistry,
            final RuntimeException failure) {
        if (segmentRegistry == null) {
            return;
        }
        try {
            segmentRegistry.close();
        } catch (final RuntimeException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private void closeKeyToSegmentMap(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final RuntimeException failure) {
        if (keyToSegmentMap == null || keyToSegmentMap.wasClosed()) {
            return;
        }
        try {
            keyToSegmentMap.close();
        } catch (final RuntimeException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

}
