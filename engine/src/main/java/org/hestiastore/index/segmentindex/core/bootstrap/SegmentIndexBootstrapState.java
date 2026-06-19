package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationCoordinator;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentLeaseService;
import org.hestiastore.index.segmentindex.core.session.SegmentTopologyRuntimeAccess;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Mutable products created during one segment-index bootstrap run.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexBootstrapState<K, V> {

    private final AtomicLong compactRequestHighWaterMark = new AtomicLong();
    private final AtomicLong flushRequestHighWaterMark = new AtomicLong();
    private final AtomicLong lastAppliedWalLsn = new AtomicLong();
    private EffectiveIndexConfiguration<K, V> configuration;
    private Boolean configurationWriteRequired;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private ExecutorRegistry executorRegistry;
    private KeyToSegmentMap<K> keyToSegmentMap;
    private ChunkStoreCache<K, V> chunkStoreCache;
    private SegmentRegistry<K, V> segmentRegistry;
    private CoreStorageRuntime<K, V> coreStorageRuntime;
    private SegmentLeaseService<K, V> segmentLeaseService;
    private SplitService<K, V> splitService;
    private SegmentTopologyRuntimeAccess<K, V> topologyRuntime;
    private WalRuntime<K, V> walRuntime;
    private MaintenanceService<K, V> maintenanceService;
    private IndexOperationCoordinator<K, V> operationAccess;
    private RuntimeTuning runtimeTuning;
    private IndexRuntimeMonitoring runtimeMonitoring;
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
        return requireInitialized(valueTypeDescriptor, "valueTypeDescriptor");
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

    void setKeyToSegmentMap(final KeyToSegmentMap<K> keyToSegmentMap) {
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
            final CoreStorageRuntime<K, V> coreStorageRuntime) {
        this.coreStorageRuntime = Vldtn.requireNonNull(coreStorageRuntime,
                "coreStorageRuntime");
    }

    RuntimeTuningState getRuntimeTuningState() {
        return getCoreStorageRuntime().getRuntimeTuningState();
    }

    KeyToSegmentMap<K> getKeyToSegmentMap() {
        return requireInitialized(keyToSegmentMap, "keyToSegmentMap");
    }

    SegmentRegistry<K, V> getSegmentRegistry() {
        return requireInitialized(segmentRegistry, "segmentRegistry");
    }

    ChunkStoreCache<K, V> getChunkStoreCache() {
        return requireInitialized(chunkStoreCache, "chunkStoreCache");
    }

    StorageService<K, V> getStorageService() {
        return getCoreStorageRuntime().getStorageService();
    }

    CoreStorageRuntime<K, V> getCoreStorageRuntime() {
        return requireInitialized(coreStorageRuntime, "coreStorageRuntime");
    }

    void setRuntimeSegmentLeaseService(
            final SegmentLeaseService<K, V> runtimeSegmentLeaseService) {
        segmentLeaseService = Vldtn.requireNonNull(runtimeSegmentLeaseService,
                "runtimeSegmentLeaseService");
    }

    SegmentLeaseService<K, V> getRuntimeSegmentLeaseService() {
        return requireInitialized(segmentLeaseService,
                "runtimeSegmentLeaseService");
    }

    void setRuntimeSplitService(
            final SplitService<K, V> runtimeSplitService) {
        splitService = Vldtn.requireNonNull(runtimeSplitService,
                "runtimeSplitService");
    }

    boolean hasRuntimeSplitService() {
        return splitService != null;
    }

    SplitService<K, V> getRuntimeSplitService() {
        return requireInitialized(splitService, "runtimeSplitService");
    }

    void setRuntimeTopologyRuntime(
            final SegmentTopologyRuntimeAccess<K, V> runtimeTopologyRuntime) {
        topologyRuntime = Vldtn.requireNonNull(runtimeTopologyRuntime,
                "runtimeTopologyRuntime");
    }

    SegmentTopologyRuntimeAccess<K, V> getRuntimeTopologyRuntime() {
        return requireInitialized(topologyRuntime, "runtimeTopologyRuntime");
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
            final IndexOperationCoordinator<K, V> runtimeOperationAccess) {
        operationAccess = Vldtn.requireNonNull(runtimeOperationAccess,
                "runtimeOperationAccess");
    }

    IndexOperationCoordinator<K, V> getRuntimeOperationAccess() {
        return requireInitialized(operationAccess, "runtimeOperationAccess");
    }

    void setRuntimeMaintenanceService(
            final MaintenanceService<K, V> runtimeMaintenanceService) {
        maintenanceService = Vldtn.requireNonNull(runtimeMaintenanceService,
                "runtimeMaintenanceService");
    }

    MaintenanceService<K, V> getRuntimeMaintenanceService() {
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

    void setRuntimeMonitoring(final IndexRuntimeMonitoring runtimeMonitoring) {
        this.runtimeMonitoring = Vldtn.requireNonNull(runtimeMonitoring,
                "runtimeMonitoring");
    }

    IndexRuntimeMonitoring getRuntimeMonitoring() {
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
