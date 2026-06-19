package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.Optional;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.chunkstorecache.LruChunkStoreCache;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.DataTypeDescriptorRegistry;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexWalConfiguration;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationManager;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationResolution;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStorage;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningConfigurationMapper;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningServiceImpl;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningSnapshot;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.configuration.tuning.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationCoordinator;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentLeaseService;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionFactory;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.hestiastore.index.segmentindex.core.session.SegmentTopologyRuntimeAccess;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationGateway;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.core.storage.WalRuntimeInitialization;
import org.hestiastore.index.segmentindex.core.streaming.DirectSegmentCoordinator;
import org.hestiastore.index.segmentindex.core.streaming.SegmentStreamingService;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.logging.IndexMdcCallWrapper;
import org.hestiastore.index.segmentindex.logging.SegmentIndexMdcLoggingAdapter;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoringBuilder;
import org.hestiastore.index.segmentindex.wal.WalMonitoringView;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Opens one segment-index bootstrap operation and owns cleanup until the
 * running index is returned to the caller.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexBootstrapOperation<K, V> {

    private static final String ERROR_INDEX_ALREADY_EXISTS = "Cannot create segment index because configuration already exists.";
    private static final String ERROR_INDEX_NOT_FOUND = "Cannot open segment index because configuration does not exist.";
    private static final String STALE_LOCK_RECOVERY_MESSAGE = "Recovered stale index lock (.lock). Index is going to be checked "
            + "for consistency and unlocked.";

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SegmentIndexBootstrapOperation.class);

    private final Directory directory;
    private final IndexConfiguration<K, V> userProvidedConfiguration;
    private final ChunkFilterProviderResolver chunkFilterProviderResolver;

    SegmentIndexBootstrapOperation(final Directory directory,
            final IndexConfiguration<K, V> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.userProvidedConfiguration = Vldtn.requireNonNull(
                userProvidedConfiguration, "userProvidedConfiguration");
        this.chunkFilterProviderResolver = chunkFilterProviderResolver;
    }

    SegmentIndex<K, V> create() {
        return openSession(SegmentIndexBootstrapMode.CREATE).orElseThrow(
                this::missingIndex);
    }

    SegmentIndex<K, V> open() {
        return openSession(SegmentIndexBootstrapMode.OPEN).orElseThrow(
                this::missingIndex);
    }

    Optional<SegmentIndex<K, V>> tryOpen() {
        return openSession(SegmentIndexBootstrapMode.TRY_OPEN);
    }

    private Optional<SegmentIndex<K, V>> openSession(
            final SegmentIndexBootstrapMode mode) {
        final SegmentIndexBootstrapState<K, V> state = new SegmentIndexBootstrapState<>();
        final SegmentIndexSessionResources<K, V> sessionResources = new SegmentIndexSessionResources<>();
        final IndexConfigurationStorage<K, V> configurationStorage =
                new IndexConfigurationStorage<>(directory,
                        resolveProviderResolver());
        final IndexConfigurationManager<K, V> configurationManager =
                new IndexConfigurationManager<>(configurationStorage);
        try {
            if (!checkExistingConfiguration(mode,
                    configurationStorage.exists())) {
                return Optional.empty();
            }
            sessionResources.acquireDirectoryLock(directory);
            resolveConfiguration(mode, state, configurationManager);
            resolveTypeDescriptors(state);
            writeConfiguration(state, configurationManager);
            createExecutorRegistry(state);
            openKeyToSegmentMap(state);
            createChunkStoreCache(state);
            openSegmentRegistry(state);
            openCoreStorage(state);
            createRuntimeTopology(state, sessionResources);
            openRuntimeWal(state);
            createMaintenance(state, sessionResources);
            initializeWal(state, sessionResources);
            createRuntimeMonitoring(state, sessionResources);
            createRuntimeTuning(state);
            createOperationAccess(state, sessionResources);
            transferRuntimeCloseOwnership(state, sessionResources);
            createIndex(state, sessionResources);
            completeStartup(state, sessionResources);
            applyContextLogging(state);
            return Optional.of(state.getIndex());
        } catch (final RuntimeException failure) {
            throw closeAfterFailedBootstrap(state, failure);
        }
    }

    private boolean checkExistingConfiguration(
            final SegmentIndexBootstrapMode mode,
            final boolean configurationExists) {
        if (configurationExists && mode == SegmentIndexBootstrapMode.CREATE) {
            throw new IndexException(ERROR_INDEX_ALREADY_EXISTS);
        }
        if (!configurationExists && mode == SegmentIndexBootstrapMode.TRY_OPEN) {
            return false;
        }
        if (!configurationExists && mode == SegmentIndexBootstrapMode.OPEN) {
            throw new IndexException(ERROR_INDEX_NOT_FOUND);
        }
        return true;
    }

    private void resolveConfiguration(
            final SegmentIndexBootstrapMode mode,
            final SegmentIndexBootstrapState<K, V> state,
            final IndexConfigurationManager<K, V> configurationManager) {
        final IndexConfigurationResolution<K, V> resolution = mode == SegmentIndexBootstrapMode.CREATE
                ? configurationManager.resolveForCreate(
                        userProvidedConfiguration)
                : configurationManager.resolveForOpen(
                        userProvidedConfiguration);
        state.setConfiguration(resolution.configuration());
        state.setConfigurationWriteRequired(resolution.writeRequired());
    }

    private void resolveTypeDescriptors(
            final SegmentIndexBootstrapState<K, V> state) {
        final EffectiveIndexConfiguration<K, V> configuration = state.getConfiguration();
        state.setKeyTypeDescriptor(DataTypeDescriptorRegistry.makeInstance(
                configuration.identity().keyTypeDescriptor()));
        state.setValueTypeDescriptor(DataTypeDescriptorRegistry.makeInstance(
                configuration.identity().valueTypeDescriptor()));
    }

    private void writeConfiguration(
            final SegmentIndexBootstrapState<K, V> state,
            final IndexConfigurationManager<K, V> configurationManager) {
        if (state.isConfigurationWriteRequired()) {
            configurationManager.save(state.getConfiguration());
        }
    }

    private ChunkFilterProviderResolver resolveProviderResolver() {
        if (chunkFilterProviderResolver != null) {
            return chunkFilterProviderResolver;
        }
        return userProvidedConfiguration.filters()
                .getChunkFilterProviderResolver();
    }

    private void createExecutorRegistry(
            final SegmentIndexBootstrapState<K, V> state) {
        final EffectiveIndexConfiguration<K, V> configuration = state.getConfiguration();
        state.setExecutorRegistry(ExecutorRegistry.builder()
                .withIndexName(configuration.identity().name())
                .withContextLoggingEnabled(
                        configuration.logging().contextEnabled())
                .withIndexMaintenanceThreads(
                        configuration.maintenance().indexThreads())
                .withSplitMaintenanceThreads(
                        configuration.maintenance().indexThreads())
                .withSegmentMaintenanceThreads(
                        configuration.maintenance().segmentThreads())
                .withRegistryMaintenanceThreads(
                        configuration.maintenance().registryLifecycleThreads())
                .withShutdownTimeoutMillis(
                        configuration.maintenance().busyTimeoutMillis())
                .build());
    }

    private void openKeyToSegmentMap(
            final SegmentIndexBootstrapState<K, V> state) {
        final KeyToSegmentMapImpl<K> keyToSegmentMapDelegate = new KeyToSegmentMapImpl<>(directory,
                state.getKeyTypeDescriptor());
        state.setKeyToSegmentMap(new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMapDelegate));
    }

    private void createChunkStoreCache(
            final SegmentIndexBootstrapState<K, V> state) {
        state.setChunkStoreCache(new LruChunkStoreCache<>(
                state.getConfiguration().chunkStoreCache().pageLimit()));
    }

    private void openSegmentRegistry(
            final SegmentIndexBootstrapState<K, V> state) {
        state.setSegmentRegistry(SegmentRegistry.<K, V>builder()
                .withDirectoryFacade(directory)
                .withKeyTypeDescriptor(state.getKeyTypeDescriptor())
                .withValueTypeDescriptor(state.getValueTypeDescriptor())
                .withConfiguration(state.getConfiguration())
                .withSegmentMaintenanceExecutor(
                        state.getExecutorRegistry()
                                .getStableSegmentMaintenanceExecutor())
                .withRegistryMaintenanceExecutor(
                        state.getExecutorRegistry()
                                .getRegistryMaintenanceExecutor())
                .withChunkStoreCache(state.getChunkStoreCache())
                .build());
    }

    private void openCoreStorage(
            final SegmentIndexBootstrapState<K, V> state) {
        final EffectiveIndexConfiguration<K, V> conf = state.getConfiguration();
        final RuntimeTuningState runtimeTuningState = RuntimeTuningState.fromConfiguration(conf);
        final EffectiveIndexMaintenanceConfiguration maintenance = conf.maintenance();
        final StorageService<K, V> storageService = StorageService.<K, V>builder()
                .withDirectoryFacade(directory)
                .withKeyToSegmentMap(state.getKeyToSegmentMap())
                .withSegmentRegistry(state.getSegmentRegistry())
                .withStorageCleanupBusyBackoffMillis(
                        maintenance.busyBackoffMillis())
                .withStorageCleanupBusyTimeoutMillis(
                        maintenance.busyTimeoutMillis())
                .withWalBackpressureBusyBackoffMillis(
                        maintenance.busyBackoffMillis())
                .withWalBackpressureBusyTimeoutMillis(
                        maintenance.busyTimeoutMillis())
                .build();
        state.setCoreStorageRuntime(new CoreStorageRuntime<>(
                runtimeTuningState, storageService,
                state.getSegmentRegistry(), state.getKeyToSegmentMap()));
    }

    private void createRuntimeTopology(
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentIndexSessionResources<K, V> sessionResources) {
        final SegmentTopology<K> segmentTopology = newSegmentTopology(state);
        final SegmentLeaseService<K, V> segmentLeaseService = newSegmentLeaseService(state, segmentTopology);
        final SplitService<K, V> splitService = newSplitService(state,
                segmentLeaseService, sessionResources);
        state.setRuntimeSegmentLeaseService(segmentLeaseService);
        state.setRuntimeSplitService(splitService);
        state.setRuntimeTopologyRuntime(newTopologyRuntime(state,
                splitService));
    }

    private SegmentTopology<K> newSegmentTopology(
            final SegmentIndexBootstrapState<K, V> state) {
        final EffectiveIndexMaintenanceConfiguration maintenance = state.getConfiguration().maintenance();
        return SegmentTopology.<K>builder()
                .snapshot(state.getKeyToSegmentMap().snapshot())
                .busyBackoffMillis(maintenance.busyBackoffMillis())
                .busyTimeoutMillis(maintenance.busyTimeoutMillis())
                .build();
    }

    private SegmentLeaseService<K, V> newSegmentLeaseService(
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentTopology<K> segmentTopology) {
        final EffectiveIndexMaintenanceConfiguration maintenance = state.getConfiguration().maintenance();
        return SegmentLeaseService.<K, V>builder()
                .keyToSegmentMap(state.getKeyToSegmentMap())
                .segmentRegistry(state.getSegmentRegistry())
                .segmentTopology(segmentTopology)
                .busyBackoffMillis(maintenance.busyBackoffMillis())
                .busyTimeoutMillis(maintenance.busyTimeoutMillis())
                .build();
    }

    private SplitService<K, V> newSplitService(
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentLeaseService<K, V> segmentLeaseService,
            final SegmentIndexSessionResources<K, V> sessionResources) {
        return SplitService.<K, V>builder()
                .conf(state.getConfiguration())
                .runtimeTuningState(state.getRuntimeTuningState())
                .keyToSegmentMap(state.getKeyToSegmentMap())
                .segmentLeaseService(segmentLeaseService)
                .segmentRegistry(state.getSegmentRegistry())
                .directoryFacade(directory)
                .splitExecutor(state.getExecutorRegistry()
                        .getSplitMaintenanceExecutor())
                .workerExecutor(state.getExecutorRegistry()
                        .getIndexMaintenanceExecutor())
                .splitPolicyScheduler(state.getExecutorRegistry()
                        .getSplitPolicyScheduler())
                .stateView(sessionResources)
                .failureReporter(sessionResources::markRuntimeFailure)
                .statsRecorder(sessionResources.splitStatsRecorder())
                .build();
    }

    private SegmentTopologyRuntimeAccess<K, V> newTopologyRuntime(
            final SegmentIndexBootstrapState<K, V> state,
            final SplitService<K, V> splitService) {
        final StableSegmentOperationGateway<K, V> stableSegmentGateway = StableSegmentOperationGateway.create(
                state.getSegmentRegistry());
        final SegmentStreamingService<K, V> streamingService = newStreamingService(state, stableSegmentGateway);
        final DirectSegmentCoordinator<K, V> directSegmentAccess = DirectSegmentCoordinator.create(state.getKeyToSegmentMap(),
                state.getSegmentRegistry(),
                state.getConfiguration().maintenance()
                        .busyBackoffMillis(),
                state.getConfiguration().maintenance()
                        .busyTimeoutMillis());
        return SegmentTopologyRuntimeAccess.create(splitService,
                streamingService, directSegmentAccess);
    }

    private SegmentStreamingService<K, V> newStreamingService(
            final SegmentIndexBootstrapState<K, V> state,
            final StableSegmentOperationGateway<K, V> stableSegmentGateway) {
        final EffectiveIndexMaintenanceConfiguration maintenance = state.getConfiguration().maintenance();
        return SegmentStreamingService.<K, V>builder()
                .keyToSegmentMap(state.getKeyToSegmentMap())
                .segmentRegistry(state.getSegmentRegistry())
                .stableSegmentGateway(stableSegmentGateway)
                .busyBackoffMillis(maintenance.busyBackoffMillis())
                .busyTimeoutMillis(maintenance.busyTimeoutMillis())
                .build();
    }

    private void openRuntimeWal(
            final SegmentIndexBootstrapState<K, V> state) {
        final EffectiveIndexWalConfiguration wal = state.getConfiguration().wal();
        if (!wal.isEnabled()) {
            return;
        }
        state.setRuntimeWalRuntime(WalRuntime.open(directory, wal,
                state.getKeyTypeDescriptor(),
                state.getValueTypeDescriptor()));
    }

    private void createMaintenance(
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentIndexSessionResources<K, V> sessionResources) {
        state.setRuntimeMaintenanceService(newMaintenance(state,
                sessionResources));
    }

    private MaintenanceService<K, V> newMaintenance(
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentIndexSessionResources<K, V> sessionResources) {
        final EffectiveIndexMaintenanceConfiguration maintenance = state.getConfiguration().maintenance();
        return MaintenanceService.<K, V>builder()
                .keyToSegmentMap(state.getKeyToSegmentMap())
                .stableSegmentGateway(StableSegmentOperationGateway.create(
                        state.getSegmentRegistry()))
                .splitService(state.getRuntimeSplitService())
                .busyBackoffMillis(maintenance.busyBackoffMillis())
                .busyTimeoutMillis(maintenance.busyTimeoutMillis())
                .statsRecorder(sessionResources.maintenanceStatsRecorder())
                .maintenanceExecutor(
                        state.getExecutorRegistry()
                                .getIndexMaintenanceExecutor())
                .storageService(state.getStorageService())
                .build();
    }

    private void initializeWal(final SegmentIndexBootstrapState<K, V> state,
            final SegmentIndexSessionResources<K, V> sessionResources) {
        final StorageService<K, V> storageService = state.getStorageService();
        storageService.initializeWal(new WalRuntimeInitialization<>(
                state.getConfiguration(),
                state.hasRuntimeWalRuntime() ? state.getRuntimeWalRuntime()
                        : null,
                state.getRuntimeMaintenanceService()::flushAndWait,
                sessionResources, sessionResources, state.lastAppliedWalLsn()));
    }

    private void createRuntimeMonitoring(
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentIndexSessionResources<K, V> sessionResources) {
        final SplitService<K, V> splitService = state.getRuntimeSplitService();
        state.setRuntimeMonitoring(newRuntimeMonitoring(state, splitService,
                sessionResources));
    }

    private IndexRuntimeMonitoring newRuntimeMonitoring(
            final SegmentIndexBootstrapState<K, V> state,
            final SplitService<K, V> splitService,
            final SegmentIndexSessionResources<K, V> sessionResources) {
        return IndexRuntimeMonitoringBuilder.<K, V>builder()
                .withConf(state.getConfiguration())
                .withKeyToSegmentMap(state.getKeyToSegmentMap())
                .withSegmentRegistry(state.getSegmentRegistry())
                .withSplitService(splitService)
                .withExecutorRegistry(state.getExecutorRegistry())
                .withRuntimeTuningState(state.getRuntimeTuningState())
                .withChunkStoreCache(state.getChunkStoreCache())
                .withWalMonitoringView(walMonitoringView(state))
                .withIndexOperationStatsRecorder(
                        sessionResources.operationStatsRecorder())
                .withMaintenanceStatsRecorder(
                        sessionResources.maintenanceStatsRecorder())
                .withCompactRequestHighWaterMark(
                        state.compactRequestHighWaterMark())
                .withFlushRequestHighWaterMark(
                        state.flushRequestHighWaterMark())
                .withLastAppliedWalLsn(state.lastAppliedWalLsn())
                .withStateView(sessionResources)
                .build();
    }

    private WalMonitoringView walMonitoringView(
            final SegmentIndexBootstrapState<K, V> state) {
        if (!state.getConfiguration().wal().isEnabled()) {
            return WalMonitoringView.empty();
        }
        return state.getRuntimeWalRuntime();
    }

    private void createRuntimeTuning(
            final SegmentIndexBootstrapState<K, V> state) {
        final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier = new SegmentRuntimeLimitApplier<>(
                state.getSegmentRegistry(),
                state.getSegmentRegistry().runtime(),
                state.getChunkStoreCache());
        state.setRuntimeTuning(newRuntimeTuning(state,
                state.getRuntimeTopologyRuntime(), runtimeLimitApplier));
    }

    private RuntimeTuning newRuntimeTuning(
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier) {
        final EffectiveIndexConfiguration<K, V> configuration = state.getConfiguration();
        return new RuntimeTuningServiceImpl(state.getRuntimeTuningState(),
                runtimeLimitApplier::apply,
                topologyRuntime::requestFullSplitScan,
                snapshot -> persistRuntimeTuning(configuration, snapshot));
    }

    private void persistRuntimeTuning(
            final EffectiveIndexConfiguration<K, V> configuration,
            final RuntimeTuningSnapshot snapshot) {
        new IndexConfigurationStorage<K, V>(directory)
                .save(RuntimeTuningConfigurationMapper.apply(configuration,
                        snapshot));
    }

    private void createOperationAccess(
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentIndexSessionResources<K, V> sessionResources) {
        state.setRuntimeOperationAccess(new IndexOperationCoordinator<>(
                state.getValueTypeDescriptor(),
                sessionResources.operationStatsRecorder(),
                state.getRuntimeSegmentLeaseService(),
                state.getStorageService()));
    }

    private void transferRuntimeCloseOwnership(
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentIndexSessionResources<K, V> sessionResources) {
        sessionResources.setExecutorRegistry(state.getExecutorRegistry());
        state.markRuntimeCloseOwnershipTransferred();
    }

    private void createIndex(final SegmentIndexBootstrapState<K, V> state,
            final SegmentIndexSessionResources<K, V> sessionResources) {
        state.setIndex(SegmentIndexSessionFactory.createIndex(
                sessionResources,
                state.getConfiguration(), state.getKeyTypeDescriptor(),
                state.getRuntimeOperationAccess(),
                state.getRuntimeTopologyRuntime(),
                state.getRuntimeMaintenanceService(),
                state.getRuntimeTuning(),
                state.getRuntimeMonitoring(),
                state.getCoreStorageRuntime()));
    }

    private void completeStartup(final SegmentIndexBootstrapState<K, V> state,
            final SegmentIndexSessionResources<K, V> sessionResources) {
        final String indexName = state.getConfiguration().identity().name();

        LOGGER.debug("Opening index '{}'.", indexName);
        state.getStorageService().recoverFromWal(
                state.getRuntimeOperationAccess()::replayWalRecord);
        state.getStorageService().cleanupOrphanedSegmentDirectories();
        sessionResources.markReady();
        if (sessionResources.wasStaleLockRecovered()) {
            LOGGER.info(STALE_LOCK_RECOVERY_MESSAGE);
            state.getStorageService().runStartupConsistencyCheck();
        }
        state.getRuntimeTopologyRuntime().requestFullSplitScan();
        LOGGER.debug("Index '{}' opened.", indexName);
    }

    private void applyContextLogging(
            final SegmentIndexBootstrapState<K, V> state) {
        final EffectiveIndexConfiguration<K, V> configuration = state.getConfiguration();
        if (!configuration.logging().contextEnabled()) {
            return;
        }
        state.setIndex(new SegmentIndexMdcLoggingAdapter<>(
                state.getIndex(), new IndexMdcCallWrapper(
                        configuration.identity().name())));
    }

    private RuntimeException closeAfterFailedBootstrap(
            final SegmentIndexBootstrapState<K, V> state,
            final RuntimeException failure) {
        RuntimeException currentFailure = failure;
        if (state.runtimeCloseOwnershipTransferred()) {
            currentFailure = closeTransferredRuntime(state, currentFailure);
        } else {
            currentFailure = closeRuntimeWal(state, currentFailure);
            currentFailure = closeRuntimeSplitService(state, currentFailure);
            currentFailure = closeSegmentRegistry(state, currentFailure);
            currentFailure = closeKeyToSegmentMap(state, currentFailure);
        }
        return closeExecutorRegistry(state, currentFailure);
    }

    private RuntimeException closeTransferredRuntime(
            final SegmentIndexBootstrapState<K, V> state,
            final RuntimeException failure) {
        RuntimeException currentFailure = closeSplitRuntime(state, failure);
        currentFailure = closeCoreStorage(state, currentFailure);
        return closeStorageWal(state, currentFailure);
    }

    private RuntimeException closeSplitRuntime(
            final SegmentIndexBootstrapState<K, V> state,
            final RuntimeException failure) {
        try {
            state.getRuntimeTopologyRuntime().closeSplitRuntime();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException closeCoreStorage(
            final SegmentIndexBootstrapState<K, V> state,
            final RuntimeException failure) {
        try {
            state.getCoreStorageRuntime().closeCoreStorage();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException closeStorageWal(
            final SegmentIndexBootstrapState<K, V> state,
            final RuntimeException failure) {
        try {
            state.getStorageService().closeWal();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException closeRuntimeWal(
            final SegmentIndexBootstrapState<K, V> state,
            final RuntimeException failure) {
        if (!state.hasRuntimeWalRuntime()) {
            return failure;
        }
        try {
            state.getRuntimeWalRuntime().close();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException closeRuntimeSplitService(
            final SegmentIndexBootstrapState<K, V> state,
            final RuntimeException failure) {
        if (!state.hasRuntimeSplitService()) {
            return failure;
        }
        try {
            state.getRuntimeSplitService().close();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException closeSegmentRegistry(
            final SegmentIndexBootstrapState<K, V> state,
            final RuntimeException failure) {
        if (!state.hasSegmentRegistry()) {
            return failure;
        }
        try {
            state.getSegmentRegistry().close();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException closeKeyToSegmentMap(
            final SegmentIndexBootstrapState<K, V> state,
            final RuntimeException failure) {
        if (!state.hasKeyToSegmentMap()
                || state.getKeyToSegmentMap().wasClosed()) {
            return failure;
        }
        try {
            state.getKeyToSegmentMap().close();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException closeExecutorRegistry(
            final SegmentIndexBootstrapState<K, V> state,
            final RuntimeException failure) {
        if (!state.hasExecutorRegistry()
                || state.getExecutorRegistry().wasClosed()) {
            return failure;
        }
        try {
            state.getExecutorRegistry().close();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException appendCleanupFailure(
            final RuntimeException failure,
            final RuntimeException cleanupFailure) {
        failure.addSuppressed(cleanupFailure);
        return failure;
    }

    private IllegalStateException missingIndex() {
        return new IllegalStateException(
                "Bootstrap result does not contain an index.");
    }
}
