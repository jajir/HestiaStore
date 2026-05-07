package org.hestiastore.index.segmentindex.core.session;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.tuning.RuntimeTuningServiceImpl;
import org.hestiastore.index.segmentindex.tuning.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.segmentaccess.SegmentAccessService;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationAccess;
import org.hestiastore.index.segmentindex.core.storage.IndexRecoveryCleanupCoordinator;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorageOpenObserver;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorageFactory;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorageOpenSpec;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexRuntimeStorage;
import org.hestiastore.index.segmentindex.core.streaming.DirectSegmentAccess;
import org.hestiastore.index.segmentindex.core.streaming.SegmentStreamingService;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopologyRuntime;
import org.hestiastore.index.segmentindex.metrics.RuntimeMetricsCollector;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.tuning.RuntimeConfiguration;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoringImpl;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Builds the runtime collaborator graph used by {@link SegmentIndexRuntime}.
 *
 * @param <K> key type
 * @param <V> value type
 */
@SuppressWarnings({ "java:S6206", "java:S6539" })
final class SegmentIndexRuntimeFactory<K, V> {

    private static final ResourceCreationObserver<?, ?> NO_OP_OBSERVER =
            new ResourceCreationObserver<>() {
            };

    /**
     * Observer notified as selected runtime resources are opened.
     *
     * @param <K> key type
     * @param <V> value type
     */
    interface ResourceCreationObserver<K, V> {

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

    private final SegmentIndexRuntimeOpenContext<K, V> openContext;
    private final ResourceCreationObserver<K, V> resourceCreationObserver;

    SegmentIndexRuntimeFactory(
            final SegmentIndexRuntimeOpenContext<K, V> openContext) {
        this(openContext, noOpObserver());
    }

    SegmentIndexRuntimeFactory(
            final SegmentIndexRuntimeOpenContext<K, V> openContext,
            final ResourceCreationObserver<K, V> resourceCreationObserver) {
        this.openContext = Vldtn.requireNonNull(openContext, "openContext");
        this.resourceCreationObserver = Vldtn.requireNonNull(
                resourceCreationObserver, "resourceCreationObserver");
    }

    @SuppressWarnings("unchecked")
    private static <K, V> ResourceCreationObserver<K, V> noOpObserver() {
        return (ResourceCreationObserver<K, V>) NO_OP_OBSERVER;
    }

    SegmentIndexRuntime<K, V> open() {
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
        return new SegmentIndexCoreStorageFactory<>(newCoreStorageRequest(),
                newCoreStorageObserver()).create();
    }

    private SegmentIndexCoreStorageOpenSpec<K, V> newCoreStorageRequest() {
        return new SegmentIndexCoreStorageOpenSpec<>(
                openContext.directoryFacade, openContext.keyTypeDescriptor,
                openContext.valueTypeDescriptor, openContext.conf,
                openContext.runtimeConfiguration,
                openContext.executorRegistry);
    }

    private SegmentIndexCoreStorageOpenObserver<K, V> newCoreStorageObserver() {
        return new SegmentIndexCoreStorageOpenObserver<>() {
            @Override
            public void onKeyToSegmentMapCreated(
                    final KeyToSegmentMap<K> keyToSegmentMap) {
                resourceCreationObserver
                        .onKeyToSegmentMapCreated(keyToSegmentMap);
            }

            @Override
            public void onSegmentRegistryCreated(
                    final SegmentRegistry<K, V> segmentRegistry) {
                resourceCreationObserver
                        .onSegmentRegistryCreated(segmentRegistry);
            }
        };
    }

    private SegmentTopologyRuntime<K, V> createTopologyRuntime(
            final SegmentIndexCoreStorage<K, V> coreStorage) {
        final SegmentTopology<K> segmentTopology = SegmentTopology.<K>builder()
                .snapshot(coreStorage.keyToSegmentMap().snapshot())
                .build();
        final StableSegmentOperationAccess<K, V> stableSegmentGateway =
                StableSegmentOperationAccess.create(
                        coreStorage.segmentRegistry());
        final SegmentAccessService<K, V> segmentAccessService =
                newSegmentAccessService(coreStorage, segmentTopology);
        final SplitService splitService = newSplitService(coreStorage,
                segmentTopology);
        final SegmentStreamingService<K, V> streamingService =
                newStreamingService(coreStorage, stableSegmentGateway);
        final DirectSegmentAccess<K, V> directSegmentAccess =
                DirectSegmentAccess.create(coreStorage.keyToSegmentMap(),
                        coreStorage.segmentRegistry(),
                        coreStorage.retryPolicy());
        final IndexRecoveryCleanupCoordinator<K, V> recoveryCleanupCoordinator =
                IndexRecoveryCleanupCoordinator.create(openContext.logger,
                        openContext.directoryFacade,
                        coreStorage.keyToSegmentMap(),
                        coreStorage.segmentRegistry(),
                        coreStorage.retryPolicy());
        return new SegmentTopologyRuntime<>(splitService, streamingService,
                segmentAccessService, directSegmentAccess,
                recoveryCleanupCoordinator);
    }

    private SegmentIndexRuntime<K, V> createRuntime(
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentTopologyRuntime<K, V> topologyRuntime,
            final WalRuntime<K, V> walRuntime) {
        final SegmentIndexRuntimeServices<K, V> serviceState =
                createRuntimeServices(coreStorage, topologyRuntime, walRuntime);
        final SegmentIndexRuntimeStorage<K, V> storage =
                new SegmentIndexRuntimeStorage<>(
                        coreStorage.runtimeTuningState(),
                        coreStorage.keyToSegmentMap(),
                        coreStorage.segmentRegistry(),
                        coreStorage.retryPolicy());
        return new SegmentIndexRuntime<>(openContext.keyTypeDescriptor, storage,
                topologyRuntime, walRuntime, serviceState);
    }

    private SegmentAccessService<K, V> newSegmentAccessService(
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentTopology<K> segmentTopology) {
        return SegmentAccessService.<K, V>builder()
                .keyToSegmentMap(coreStorage.keyToSegmentMap())
                .segmentRegistry(coreStorage.segmentRegistry())
                .segmentTopology(segmentTopology)
                .retryPolicy(coreStorage.retryPolicy())
                .build();
    }

    private SplitService newSplitService(
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentTopology<K> segmentTopology) {
        return SplitService.<K, V>builder()
                .conf(openContext.conf)
                .runtimeTuningState(coreStorage.runtimeTuningState())
                .keyToSegmentMap(coreStorage.keyToSegmentMap())
                .segmentTopology(segmentTopology)
                .segmentRegistry(coreStorage.segmentRegistry())
                .directoryFacade(openContext.directoryFacade)
                .splitExecutor(openContext.executorRegistry
                        .getSplitMaintenanceExecutor())
                .workerExecutor(openContext.executorRegistry
                        .getIndexMaintenanceExecutor())
                .splitPolicyScheduler(openContext.executorRegistry
                        .getSplitPolicyScheduler())
                .stateSupplier(openContext.stateSupplier)
                .failureHandler(openContext.failureHandler)
                .stats(openContext.stats)
                .build();
    }

    private SegmentStreamingService<K, V> newStreamingService(
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final StableSegmentOperationAccess<K, V> stableSegmentGateway) {
        return SegmentStreamingService.<K, V>builder()
                .logger(openContext.logger)
                .keyToSegmentMap(coreStorage.keyToSegmentMap())
                .segmentRegistry(coreStorage.segmentRegistry())
                .stableSegmentGateway(stableSegmentGateway)
                .retryPolicy(coreStorage.retryPolicy())
                .build();
    }

    private SegmentIndexRuntimeServices<K, V> createRuntimeServices(
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentTopologyRuntime<K, V> topologyRuntime,
            final WalRuntime<K, V> walRuntime) {
        final AtomicReference<Runnable> checkpointAction =
                new AtomicReference<>(() -> {
                });
        final MaintenanceService maintenance = createMaintenance(coreStorage,
                topologyRuntime, () -> checkpointAction.get().run());
        final IndexWalCoordinator<K, V> walCoordinator = IndexWalCoordinator
                .create(openContext.logger, openContext.conf, walRuntime,
                        coreStorage.retryPolicy(), () -> { },
                        maintenance::flushAndWait, openContext.stateSupplier,
                        openContext.failureHandler,
                        openContext.lastAppliedWalLsn);
        checkpointAction.set(walCoordinator::checkpoint);
        final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier =
                new SegmentRuntimeLimitApplier<>(coreStorage.segmentRegistry(),
                        coreStorage.segmentRegistry().runtime());
        final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier =
                createMetricsSnapshotSupplier(coreStorage, topologyRuntime,
                        walRuntime);
        final IndexRuntimeMonitoring runtimeMonitoring =
                new IndexRuntimeMonitoringImpl(openContext.conf,
                        openContext.stateSupplier, metricsSnapshotSupplier);
        final RuntimeConfiguration runtimeConfiguration =
                new RuntimeTuningServiceImpl(coreStorage.runtimeTuningState(),
                        runtimeLimitApplier::apply,
                        topologyRuntime::requestFullSplitScan);
        return new SegmentIndexRuntimeServices<>(walCoordinator,
                createOperationAccess(topologyRuntime, walCoordinator),
                maintenance, runtimeLimitApplier, metricsSnapshotSupplier,
                runtimeMonitoring, runtimeConfiguration);
    }

    private MaintenanceService createMaintenance(
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentTopologyRuntime<K, V> topologyRuntime,
            final Runnable checkpointAction) {
        return MaintenanceService.<K, V>builder()
                .logger(openContext.logger)
                .keyToSegmentMap(coreStorage.keyToSegmentMap())
                .stableSegmentGateway(StableSegmentOperationAccess.create(
                        coreStorage.segmentRegistry()))
                .splitService(topologyRuntime.splitService())
                .retryPolicy(coreStorage.retryPolicy())
                .stats(openContext.stats)
                .maintenanceExecutor(openContext.executorRegistry
                        .getIndexMaintenanceExecutor())
                .checkpointAction(checkpointAction)
                .build();
    }

    private SegmentIndexOperationAccess<K, V> createOperationAccess(
            final SegmentTopologyRuntime<K, V> topologyRuntime,
            final IndexWalCoordinator<K, V> walCoordinator) {
        return SegmentIndexOperationAccess.create(
                openContext.valueTypeDescriptor, openContext.stats,
                topologyRuntime.segmentAccessService(), walCoordinator);
    }

    private Supplier<SegmentIndexMetricsSnapshot> createMetricsSnapshotSupplier(
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentTopologyRuntime<K, V> topologyRuntime,
            final WalRuntime<K, V> walRuntime) {
        final RuntimeMetricsCollector runtimeMetricsCollector =
                RuntimeMetricsCollector.<K, V>builder()
                        .withConf(openContext.conf)
                        .withKeyToSegmentMap(coreStorage.keyToSegmentMap())
                        .withSegmentRegistry(coreStorage.segmentRegistry())
                        .withSplitSnapshotSupplier(() -> topologyRuntime
                                .splitService().splitMetricsView()
                                .metricsSnapshot())
                        .withExecutorRegistry(openContext.executorRegistry)
                        .withRuntimeTuningState(
                                coreStorage.runtimeTuningState())
                        .withWalRuntime(walRuntime)
                        .withStats(openContext.stats)
                        .withCompactRequestHighWaterMark(
                                openContext.compactRequestHighWaterMark)
                        .withFlushRequestHighWaterMark(
                                openContext.flushRequestHighWaterMark)
                        .withLastAppliedWalLsn(openContext.lastAppliedWalLsn)
                        .withStateSupplier(openContext.stateSupplier)
                        .build();
        return runtimeMetricsCollector::metricsSnapshot;
    }

    private WalRuntime<K, V> openWalRuntime() {
        return WalRuntime.open(openContext.directoryFacade,
                openContext.conf.wal(), openContext.keyTypeDescriptor,
                openContext.valueTypeDescriptor);
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
