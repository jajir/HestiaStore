package org.hestiastore.index.segmentindex.core.session;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.chunkstorecache.LruChunkStoreCache;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.DataTypeDescriptorRegistry;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationManager;
import org.hestiastore.index.segmentindex.configuration.persistence.ResolvedIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStore;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.DefaultRuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeSegmentLimitApplier;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.execution.MappedSegmentMaintenanceService;
import org.hestiastore.index.segmentindex.core.execution.PointOperationCoordinator;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLeaseService;
import org.hestiastore.index.segmentindex.core.split.SplitRuntime;
import org.hestiastore.index.segmentindex.core.execution.NonBlockingSegmentOperationGateway;
import org.hestiastore.index.segmentindex.core.storage.OpenedStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageCoordinator;
import org.hestiastore.index.segmentindex.core.execution.SegmentIteratorService;
import org.hestiastore.index.segmentindex.core.routing.RouteTopology;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.PersistentSegmentRouteMap;
import org.hestiastore.index.segmentindex.monitoring.SegmentIndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.monitoring.SegmentIndexRuntimeSnapshotCollector;
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
public final class SegmentIndexBootstrapOperation<K, V> {

    private static final String ERROR_INDEX_ALREADY_EXISTS = "Cannot create segment index because configuration already exists.";
    private static final String ERROR_INDEX_NOT_FOUND = "Cannot open segment index because configuration does not exist.";
    private static final String STALE_LOCK_RECOVERY_MESSAGE = "Recovered stale index lock (.lock). Index is going to be checked "
            + "for consistency and unlocked.";

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SegmentIndexBootstrapOperation.class);

    private final Directory directory;
    private final IndexConfiguration<K, V> userProvidedConfiguration;
    private final ChunkFilterProviderResolver chunkFilterProviderResolver;

    /**
     * Creates one bootstrap operation for a segment index.
     *
     * @param directory index directory
     * @param userProvidedConfiguration user configuration overrides
     * @param chunkFilterProviderResolver optional chunk filter provider
     *        resolver
     */
    public SegmentIndexBootstrapOperation(final Directory directory,
            final IndexConfiguration<K, V> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.userProvidedConfiguration = Vldtn.requireNonNull(
                userProvidedConfiguration, "userProvidedConfiguration");
        this.chunkFilterProviderResolver = chunkFilterProviderResolver;
    }

    /**
     * Creates a new index.
     *
     * @return created index
     */
    public SegmentIndex<K, V> create() {
        return openSession(SegmentIndexBootstrapMode.CREATE).orElseThrow(
                this::missingIndex);
    }

    /**
     * Opens an existing index.
     *
     * @return opened index
     */
    public SegmentIndex<K, V> open() {
        return openSession(SegmentIndexBootstrapMode.OPEN).orElseThrow(
                this::missingIndex);
    }

    /**
     * Opens an existing index when its configuration exists.
     *
     * @return opened index, or empty when no persisted configuration exists
     */
    public Optional<SegmentIndex<K, V>> tryOpen() {
        return openSession(SegmentIndexBootstrapMode.TRY_OPEN);
    }

    private Optional<SegmentIndex<K, V>> openSession(
            final SegmentIndexBootstrapMode mode) {
        final BootstrapState<K, V> state = new BootstrapState<>();
        final SegmentIndexRuntimeResources<K, V> sessionResources = new SegmentIndexRuntimeResources<>();
        final IndexConfigurationStore<K, V> configurationStorage =
                new IndexConfigurationStore<>(directory,
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
            final BootstrapState<K, V> state,
            final IndexConfigurationManager<K, V> configurationManager) {
        final ResolvedIndexConfiguration<K, V> resolution = mode == SegmentIndexBootstrapMode.CREATE
                ? configurationManager.resolveForCreate(
                        userProvidedConfiguration)
                : configurationManager.resolveForOpen(
                        userProvidedConfiguration);
        state.setConfiguration(resolution.configuration());
        state.setConfigurationWriteRequired(resolution.writeRequired());
    }

    private void resolveTypeDescriptors(
            final BootstrapState<K, V> state) {
        final EffectiveIndexConfiguration<K, V> configuration = state.getConfiguration();
        state.setKeyTypeDescriptor(DataTypeDescriptorRegistry.makeInstance(
                configuration.identity().keyTypeDescriptor()));
        state.setValueTypeDescriptor(DataTypeDescriptorRegistry.makeInstance(
                configuration.identity().valueTypeDescriptor()));
    }

    private void writeConfiguration(
            final BootstrapState<K, V> state,
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
            final BootstrapState<K, V> state) {
        final EffectiveIndexConfiguration<K, V> configuration = state.getConfiguration();
        state.setExecutorRegistry(ExecutorRegistry.create(
                configuration.identity().name(),
                configuration.logging().contextEnabled(),
                configuration.maintenance().indexThreads(),
                configuration.maintenance().indexThreads(),
                configuration.maintenance().segmentThreads(),
                configuration.maintenance().registryLifecycleThreads(),
                configuration.maintenance().busyTimeoutMillis()));
    }

    private void openKeyToSegmentMap(
            final BootstrapState<K, V> state) {
        state.setKeyToSegmentMap(new PersistentSegmentRouteMap<>(directory,
                state.getKeyTypeDescriptor()));
    }

    private void createChunkStoreCache(
            final BootstrapState<K, V> state) {
        state.setChunkStoreCache(new LruChunkStoreCache<>(
                state.getConfiguration().chunkStoreCache().pageLimit()));
    }

    private void openSegmentRegistry(
            final BootstrapState<K, V> state) {
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
            final BootstrapState<K, V> state) {
        final EffectiveIndexConfiguration<K, V> conf = state.getConfiguration();
        final RuntimeTuningState runtimeTuningState = RuntimeTuningState.fromConfiguration(conf);
        final EffectiveIndexMaintenanceConfiguration maintenance = conf.maintenance();
        final StorageCoordinator<K, V> storageService = StorageCoordinator.create(
                directory, state.getKeyToSegmentMap(),
                state.getSegmentRegistry(), maintenance);
        state.setCoreStorageRuntime(new OpenedStorageRuntime<>(
                runtimeTuningState, storageService,
                state.getSegmentRegistry(), state.getKeyToSegmentMap()));
    }

    private void createRuntimeTopology(
            final BootstrapState<K, V> state,
            final SegmentIndexRuntimeResources<K, V> sessionResources) {
        final RouteTopology<K> segmentTopology = newSegmentTopology(state);
        final MappedSegmentLeaseService<K, V> segmentLeaseService = newSegmentLeaseService(state, segmentTopology);
        final SplitRuntime<K, V> splitService = newSplitService(state,
                segmentLeaseService, sessionResources);
        state.setRuntimeSegmentLeaseService(segmentLeaseService);
        state.setRuntimeSplitService(splitService);
        state.setRuntimeStreamingService(newStreamingService(state));
    }

    private RouteTopology<K> newSegmentTopology(
            final BootstrapState<K, V> state) {
        final EffectiveIndexMaintenanceConfiguration maintenance = state.getConfiguration().maintenance();
        return RouteTopology.create(state.getKeyToSegmentMap().snapshot(),
                maintenance.busyBackoffMillis(),
                maintenance.busyTimeoutMillis());
    }

    private MappedSegmentLeaseService<K, V> newSegmentLeaseService(
            final BootstrapState<K, V> state,
            final RouteTopology<K> segmentTopology) {
        final EffectiveIndexMaintenanceConfiguration maintenance = state.getConfiguration().maintenance();
        return MappedSegmentLeaseService.create(state.getKeyToSegmentMap(),
                state.getSegmentRegistry(), segmentTopology,
                maintenance.busyBackoffMillis(),
                maintenance.busyTimeoutMillis());
    }

    private SplitRuntime<K, V> newSplitService(
            final BootstrapState<K, V> state,
            final MappedSegmentLeaseService<K, V> segmentLeaseService,
            final SegmentIndexRuntimeResources<K, V> sessionResources) {
        return SplitRuntime.create(state.getConfiguration(),
                state.getRuntimeTuningState(), state.getKeyToSegmentMap(),
                segmentLeaseService, state.getSegmentRegistry(), directory,
                state.getExecutorRegistry().getSplitMaintenanceExecutor(),
                state.getExecutorRegistry().getIndexMaintenanceExecutor(),
                state.getExecutorRegistry().getSplitPolicyScheduler(),
                sessionResources, sessionResources.splitStatsRecorder());
    }

    private SegmentIteratorService<K, V> newStreamingService(
            final BootstrapState<K, V> state) {
        final EffectiveIndexMaintenanceConfiguration maintenance = state.getConfiguration().maintenance();
        return SegmentIteratorService.create(
                state.getRuntimeSegmentLeaseService(),
                maintenance.busyBackoffMillis(),
                maintenance.busyTimeoutMillis());
    }

    private void openRuntimeWal(
            final BootstrapState<K, V> state) {
        final IndexWalConfiguration wal = state.getConfiguration().wal();
        if (!wal.isEnabled()) {
            return;
        }
        state.setRuntimeWalRuntime(WalRuntime.open(directory, wal,
                state.getKeyTypeDescriptor(),
                state.getValueTypeDescriptor()));
    }

    private void createMaintenance(
            final BootstrapState<K, V> state,
            final SegmentIndexRuntimeResources<K, V> sessionResources) {
        state.setRuntimeMaintenanceService(newMaintenance(state,
                sessionResources));
    }

    private MappedSegmentMaintenanceService<K, V> newMaintenance(
            final BootstrapState<K, V> state,
            final SegmentIndexRuntimeResources<K, V> sessionResources) {
        final EffectiveIndexMaintenanceConfiguration maintenance = state.getConfiguration().maintenance();
        return MappedSegmentMaintenanceService.create(state.getKeyToSegmentMap(),
                NonBlockingSegmentOperationGateway.create(
                        state.getSegmentRegistry()),
                state.getRuntimeSplitService(), maintenance,
                sessionResources.maintenanceStatsRecorder(),
                state.getExecutorRegistry().getIndexMaintenanceExecutor(),
                state.getStorageService());
    }

    private void initializeWal(final BootstrapState<K, V> state,
            final SegmentIndexRuntimeResources<K, V> sessionResources) {
        final StorageCoordinator<K, V> storageService = state.getStorageService();
        storageService.initializeWal(state.getConfiguration(),
                state.hasRuntimeWalRuntime() ? state.getRuntimeWalRuntime()
                        : null,
                state.getRuntimeMaintenanceService(), sessionResources,
                state.lastAppliedWalLsn());
    }

    private void createRuntimeMonitoring(
            final BootstrapState<K, V> state,
            final SegmentIndexRuntimeResources<K, V> sessionResources) {
        final SplitRuntime<K, V> splitService = state.getRuntimeSplitService();
        state.setRuntimeMonitoring(newRuntimeMonitoring(state, splitService,
                sessionResources));
    }

    private SegmentIndexRuntimeMonitoring newRuntimeMonitoring(
            final BootstrapState<K, V> state,
            final SplitRuntime<K, V> splitService,
            final SegmentIndexRuntimeResources<K, V> sessionResources) {
        return SegmentIndexRuntimeSnapshotCollector.create(state.getConfiguration(),
                state.getKeyToSegmentMap(), state.getSegmentRegistry(),
                splitService, state.getExecutorRegistry(),
                state.getRuntimeTuningState(), state.getChunkStoreCache(),
                walMonitoringView(state),
                sessionResources.operationStatsRecorder(),
                sessionResources.maintenanceStatsRecorder(),
                state.compactRequestHighWaterMark(),
                state.flushRequestHighWaterMark(), state.lastAppliedWalLsn(),
                sessionResources);
    }

    private WalMonitoringView walMonitoringView(
            final BootstrapState<K, V> state) {
        if (!state.getConfiguration().wal().isEnabled()) {
            return WalMonitoringView.empty();
        }
        return state.getRuntimeWalRuntime();
    }

    private void createRuntimeTuning(
            final BootstrapState<K, V> state) {
        final RuntimeSegmentLimitApplier<K, V> runtimeLimitApplier = new RuntimeSegmentLimitApplier<>(
                state.getSegmentRegistry(),
                state.getSegmentRegistry().runtime(),
                state.getChunkStoreCache());
        state.setRuntimeTuning(newRuntimeTuning(state, runtimeLimitApplier));
    }

    private RuntimeTuning newRuntimeTuning(
            final BootstrapState<K, V> state,
            final RuntimeSegmentLimitApplier<K, V> runtimeLimitApplier) {
        final EffectiveIndexConfiguration<K, V> configuration = state.getConfiguration();
        return new DefaultRuntimeTuning<>(state.getRuntimeTuningState(),
                runtimeLimitApplier, state.getRuntimeSplitService(),
                configuration, new IndexConfigurationStore<>(directory));
    }

    private void createOperationAccess(
            final BootstrapState<K, V> state,
            final SegmentIndexRuntimeResources<K, V> sessionResources) {
        state.setRuntimeOperationAccess(new PointOperationCoordinator<>(
                state.getValueTypeDescriptor(),
                sessionResources.operationStatsRecorder(),
                state.getRuntimeSegmentLeaseService(),
                state.getStorageService()));
    }

    private void transferRuntimeCloseOwnership(
            final BootstrapState<K, V> state,
            final SegmentIndexRuntimeResources<K, V> sessionResources) {
        sessionResources.setExecutorRegistry(state.getExecutorRegistry());
        state.markRuntimeCloseOwnershipTransferred();
    }

    private void createIndex(final BootstrapState<K, V> state,
            final SegmentIndexRuntimeResources<K, V> sessionResources) {
        state.setIndex(SegmentIndexSessionAssembler.createIndex(
                sessionResources,
                state.getConfiguration(), state.getKeyTypeDescriptor(),
                state.getRuntimeOperationAccess(),
                state.getRuntimeSplitService(),
                state.getRuntimeStreamingService(),
                state.getRuntimeMaintenanceService(),
                state.getRuntimeTuning(),
                state.getRuntimeMonitoring(),
                state.getCoreStorageRuntime()));
    }

    private void completeStartup(final BootstrapState<K, V> state,
            final SegmentIndexRuntimeResources<K, V> sessionResources) {
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
        state.getRuntimeSplitService().requestFullSplitScan();
        LOGGER.debug("Index '{}' opened.", indexName);
    }

    private void applyContextLogging(
            final BootstrapState<K, V> state) {
        final EffectiveIndexConfiguration<K, V> configuration = state.getConfiguration();
        if (!configuration.logging().contextEnabled()) {
            return;
        }
        state.setIndex(new SegmentIndexMdcLoggingAdapter<>(
                state.getIndex(), configuration.identity().name()));
    }

    private RuntimeException closeAfterFailedBootstrap(
            final BootstrapState<K, V> state,
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
            final BootstrapState<K, V> state,
            final RuntimeException failure) {
        RuntimeException currentFailure = closeSplitRuntime(state, failure);
        currentFailure = closeCoreStorage(state, currentFailure);
        return closeStorageWal(state, currentFailure);
    }

    private RuntimeException closeSplitRuntime(
            final BootstrapState<K, V> state,
            final RuntimeException failure) {
        try {
            state.getRuntimeSplitService().close();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException closeCoreStorage(
            final BootstrapState<K, V> state,
            final RuntimeException failure) {
        try {
            state.getCoreStorageRuntime().closeCoreStorage();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException closeStorageWal(
            final BootstrapState<K, V> state,
            final RuntimeException failure) {
        try {
            state.getStorageService().closeWal();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException closeRuntimeWal(
            final BootstrapState<K, V> state,
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
            final BootstrapState<K, V> state,
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
            final BootstrapState<K, V> state,
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
            final BootstrapState<K, V> state,
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
            final BootstrapState<K, V> state,
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

    private static final class BootstrapState<K, V> {

        private final AtomicLong compactRequestHighWaterMark =
                new AtomicLong();
        private final AtomicLong flushRequestHighWaterMark = new AtomicLong();
        private final AtomicLong lastAppliedWalLsn = new AtomicLong();
        private EffectiveIndexConfiguration<K, V> configuration;
        private Boolean configurationWriteRequired;
        private TypeDescriptor<K> keyTypeDescriptor;
        private TypeDescriptor<V> valueTypeDescriptor;
        private ExecutorRegistry executorRegistry;
        private SegmentRouteMap<K> keyToSegmentMap;
        private ChunkStoreCache<K, V> chunkStoreCache;
        private SegmentRegistry<K, V> segmentRegistry;
        private OpenedStorageRuntime<K, V> coreStorageRuntime;
        private MappedSegmentLeaseService<K, V> segmentLeaseService;
        private SplitRuntime<K, V> splitService;
        private SegmentIteratorService<K, V> streamingService;
        private WalRuntime<K, V> walRuntime;
        private MappedSegmentMaintenanceService<K, V> maintenanceService;
        private PointOperationCoordinator<K, V> operationAccess;
        private RuntimeTuning runtimeTuning;
        private SegmentIndexRuntimeMonitoring runtimeMonitoring;
        private SegmentIndex<K, V> index;
        private boolean closeOwnershipTransferred;

        void setConfiguration(
                final EffectiveIndexConfiguration<K, V> configuration) {
            this.configuration = Vldtn.requireNonNull(configuration,
                    "configuration");
        }

        EffectiveIndexConfiguration<K, V> getConfiguration() {
            return requireInitialized(configuration, "configuration");
        }

        void setConfigurationWriteRequired(
                final boolean configurationWriteRequired) {
            this.configurationWriteRequired = configurationWriteRequired;
        }

        boolean isConfigurationWriteRequired() {
            return requireInitialized(configurationWriteRequired,
                    "configurationWriteRequired");
        }

        void setKeyTypeDescriptor(
                final TypeDescriptor<K> keyTypeDescriptor) {
            this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                    "keyTypeDescriptor");
        }

        TypeDescriptor<K> getKeyTypeDescriptor() {
            return requireInitialized(keyTypeDescriptor, "keyTypeDescriptor");
        }

        void setValueTypeDescriptor(
                final TypeDescriptor<V> valueTypeDescriptor) {
            this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                    "valueTypeDescriptor");
        }

        TypeDescriptor<V> getValueTypeDescriptor() {
            return requireInitialized(valueTypeDescriptor,
                    "valueTypeDescriptor");
        }

        void setExecutorRegistry(final ExecutorRegistry executorRegistry) {
            this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                    "executorRegistry");
        }

        ExecutorRegistry getExecutorRegistry() {
            return requireInitialized(executorRegistry, "executorRegistry");
        }

        boolean hasExecutorRegistry() {
            return executorRegistry != null;
        }

        void setIndex(final SegmentIndex<K, V> index) {
            this.index = Vldtn.requireNonNull(index, "index");
        }

        SegmentIndex<K, V> getIndex() {
            return requireInitialized(index, "index");
        }

        AtomicLong compactRequestHighWaterMark() {
            return compactRequestHighWaterMark;
        }

        AtomicLong flushRequestHighWaterMark() {
            return flushRequestHighWaterMark;
        }

        AtomicLong lastAppliedWalLsn() {
            return lastAppliedWalLsn;
        }

        void setKeyToSegmentMap(final SegmentRouteMap<K> keyToSegmentMap) {
            this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                    "keyToSegmentMap");
        }

        boolean hasKeyToSegmentMap() {
            return keyToSegmentMap != null;
        }

        void setChunkStoreCache(
                final ChunkStoreCache<K, V> chunkStoreCache) {
            this.chunkStoreCache = Vldtn.requireNonNull(chunkStoreCache,
                    "chunkStoreCache");
        }

        void setSegmentRegistry(
                final SegmentRegistry<K, V> segmentRegistry) {
            this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                    "segmentRegistry");
        }

        boolean hasSegmentRegistry() {
            return segmentRegistry != null;
        }

        void setCoreStorageRuntime(
                final OpenedStorageRuntime<K, V> coreStorageRuntime) {
            this.coreStorageRuntime = Vldtn.requireNonNull(coreStorageRuntime,
                    "coreStorageRuntime");
        }

        RuntimeTuningState getRuntimeTuningState() {
            return getCoreStorageRuntime().getRuntimeTuningState();
        }

        SegmentRouteMap<K> getKeyToSegmentMap() {
            return requireInitialized(keyToSegmentMap, "keyToSegmentMap");
        }

        SegmentRegistry<K, V> getSegmentRegistry() {
            return requireInitialized(segmentRegistry, "segmentRegistry");
        }

        ChunkStoreCache<K, V> getChunkStoreCache() {
            return requireInitialized(chunkStoreCache, "chunkStoreCache");
        }

        StorageCoordinator<K, V> getStorageService() {
            return getCoreStorageRuntime().getStorageService();
        }

        OpenedStorageRuntime<K, V> getCoreStorageRuntime() {
            return requireInitialized(coreStorageRuntime, "coreStorageRuntime");
        }

        void setRuntimeSegmentLeaseService(
                final MappedSegmentLeaseService<K, V> runtimeSegmentLeaseService) {
            segmentLeaseService = Vldtn.requireNonNull(
                    runtimeSegmentLeaseService, "runtimeSegmentLeaseService");
        }

        MappedSegmentLeaseService<K, V> getRuntimeSegmentLeaseService() {
            return requireInitialized(segmentLeaseService,
                    "runtimeSegmentLeaseService");
        }

        void setRuntimeSplitService(
                final SplitRuntime<K, V> runtimeSplitService) {
            splitService = Vldtn.requireNonNull(runtimeSplitService,
                    "runtimeSplitService");
        }

        boolean hasRuntimeSplitService() {
            return splitService != null;
        }

        SplitRuntime<K, V> getRuntimeSplitService() {
            return requireInitialized(splitService, "runtimeSplitService");
        }

        void setRuntimeStreamingService(
                final SegmentIteratorService<K, V> runtimeStreamingService) {
            streamingService = Vldtn.requireNonNull(runtimeStreamingService,
                    "runtimeStreamingService");
        }

        SegmentIteratorService<K, V> getRuntimeStreamingService() {
            return requireInitialized(streamingService,
                    "runtimeStreamingService");
        }

        void setRuntimeWalRuntime(final WalRuntime<K, V> runtimeWalRuntime) {
            walRuntime = Vldtn.requireNonNull(runtimeWalRuntime,
                    "runtimeWalRuntime");
        }

        boolean hasRuntimeWalRuntime() {
            return walRuntime != null;
        }

        WalRuntime<K, V> getRuntimeWalRuntime() {
            return requireInitialized(walRuntime, "runtimeWalRuntime");
        }

        void setRuntimeOperationAccess(
                final PointOperationCoordinator<K, V> runtimeOperationAccess) {
            operationAccess = Vldtn.requireNonNull(runtimeOperationAccess,
                    "runtimeOperationAccess");
        }

        PointOperationCoordinator<K, V> getRuntimeOperationAccess() {
            return requireInitialized(operationAccess,
                    "runtimeOperationAccess");
        }

        void setRuntimeMaintenanceService(
                final MappedSegmentMaintenanceService<K, V> runtimeMaintenanceService) {
            maintenanceService = Vldtn.requireNonNull(runtimeMaintenanceService,
                    "runtimeMaintenanceService");
        }

        MappedSegmentMaintenanceService<K, V> getRuntimeMaintenanceService() {
            return requireInitialized(maintenanceService,
                    "runtimeMaintenanceService");
        }

        void setRuntimeTuning(final RuntimeTuning runtimeTuning) {
            this.runtimeTuning = Vldtn.requireNonNull(runtimeTuning,
                    "runtimeTuning");
        }

        RuntimeTuning getRuntimeTuning() {
            return requireInitialized(runtimeTuning, "runtimeTuning");
        }

        void setRuntimeMonitoring(
                final SegmentIndexRuntimeMonitoring runtimeMonitoring) {
            this.runtimeMonitoring = Vldtn.requireNonNull(runtimeMonitoring,
                    "runtimeMonitoring");
        }

        SegmentIndexRuntimeMonitoring getRuntimeMonitoring() {
            return requireInitialized(runtimeMonitoring, "runtimeMonitoring");
        }

        void markRuntimeCloseOwnershipTransferred() {
            closeOwnershipTransferred = true;
        }

        boolean runtimeCloseOwnershipTransferred() {
            return closeOwnershipTransferred;
        }

        private static <T> T requireInitialized(final T value,
                final String fieldName) {
            if (value == null) {
                throw new IllegalStateException(
                        fieldName + " was not initialized.");
            }
            return value;
        }
    }
}
