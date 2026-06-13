package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentLeaseService;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexResourceClosingAdapter;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResource;
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
    private final BootstrapStorageState<K, V> storageState =
            new BootstrapStorageState<>();
    private final BootstrapRuntimeState<K, V> runtimeState =
            new BootstrapRuntimeState<>();
    private EffectiveIndexConfiguration<K, V> configuration;
    private Boolean configurationWriteRequired;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private ExecutorRegistry executorRegistry;
    private SegmentIndexSessionResource<K, V> indexHandle;
    private SegmentIndexResourceClosingAdapter<K, V> index;
    private SegmentIndexBootstrapResult<K, V> result;

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

    void setIndexHandle(
            final SegmentIndexSessionResource<K, V> indexHandle) {
        this.indexHandle = Vldtn.requireNonNull(indexHandle, "indexHandle");
    }

    SegmentIndexSessionResource<K, V> getIndexHandle() {
        return requireInitialized(indexHandle, "indexHandle");
    }

    void setIndex(final SegmentIndexResourceClosingAdapter<K, V> index) {
        this.index = Vldtn.requireNonNull(index, "index");
    }

    SegmentIndexResourceClosingAdapter<K, V> getIndex() {
        return requireInitialized(index, "index");
    }

    void setResult(final SegmentIndexBootstrapResult<K, V> result) {
        this.result = Vldtn.requireNonNull(result, "result");
    }

    boolean hasResult() {
        return result != null;
    }

    SegmentIndexBootstrapResult<K, V> getResult() {
        return requireInitialized(result, "result");
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
        storageState.setKeyToSegmentMap(keyToSegmentMap);
    }

    boolean hasKeyToSegmentMap() {
        return storageState.hasKeyToSegmentMap();
    }

    void setChunkStoreCache(
            final ChunkStoreCache<K, V> chunkStoreCache) {
        storageState.setChunkStoreCache(chunkStoreCache);
    }

    void setSegmentRegistry(
            final SegmentRegistry<K, V> segmentRegistry) {
        storageState.setSegmentRegistry(segmentRegistry);
    }

    void setCoreStorageRuntime(
            final CoreStorageRuntime<K, V> coreStorageRuntime) {
        storageState.setCoreStorageRuntime(coreStorageRuntime);
    }

    boolean hasCoreStorage() {
        return storageState.hasCoreStorage();
    }

    RuntimeTuningState getRuntimeTuningState() {
        return storageState.getRuntimeTuningState();
    }

    KeyToSegmentMap<K> getKeyToSegmentMap() {
        return storageState.getKeyToSegmentMap();
    }

    SegmentRegistry<K, V> getSegmentRegistry() {
        return storageState.getSegmentRegistry();
    }

    ChunkStoreCache<K, V> getChunkStoreCache() {
        return storageState.getChunkStoreCache();
    }

    StorageService<K, V> getStorageService() {
        return storageState.getStorageService();
    }

    CoreStorageRuntime<K, V> getCoreStorageRuntime() {
        return storageState.getCoreStorageRuntime();
    }

    void setRuntimeSegmentLeaseService(
            final SegmentLeaseService<K, V> runtimeSegmentLeaseService) {
        runtimeState.setSegmentLeaseService(runtimeSegmentLeaseService);
    }

    SegmentLeaseService<K, V> getRuntimeSegmentLeaseService() {
        return runtimeState.getSegmentLeaseService();
    }

    void setRuntimeSplitService(final SplitService runtimeSplitService) {
        runtimeState.setSplitService(runtimeSplitService);
    }

    boolean hasRuntimeSplitService() {
        return runtimeState.hasSplitService();
    }

    SplitService getRuntimeSplitService() {
        return runtimeState.getSplitService();
    }

    void setRuntimeTopologyRuntime(
            final SegmentTopologyRuntimeAccess<K, V> runtimeTopologyRuntime) {
        runtimeState.setTopologyRuntime(runtimeTopologyRuntime);
    }

    SegmentTopologyRuntimeAccess<K, V> getRuntimeTopologyRuntime() {
        return runtimeState.getTopologyRuntime();
    }

    void setRuntimeWalRuntime(final WalRuntime<K, V> runtimeWalRuntime) {
        runtimeState.setWalRuntime(runtimeWalRuntime);
    }

    boolean hasRuntimeWalRuntime() {
        return runtimeState.hasWalRuntime();
    }

    WalRuntime<K, V> getRuntimeWalRuntime() {
        return runtimeState.getWalRuntime();
    }

    void setRuntimeMaintenanceCheckpoint(
            final BootstrapWalCheckpoint runtimeMaintenanceCheckpoint) {
        runtimeState.setMaintenanceCheckpoint(runtimeMaintenanceCheckpoint);
    }

    BootstrapWalCheckpoint getRuntimeMaintenanceCheckpoint() {
        return runtimeState.getMaintenanceCheckpoint();
    }

    void setRuntimeOperationAccess(
            final SegmentIndexOperationAccess<K, V> runtimeOperationAccess) {
        runtimeState.setOperationAccess(runtimeOperationAccess);
    }

    SegmentIndexOperationAccess<K, V> getRuntimeOperationAccess() {
        return runtimeState.getOperationAccess();
    }

    void setRuntimeMaintenanceService(
            final MaintenanceService runtimeMaintenanceService) {
        runtimeState.setMaintenanceService(runtimeMaintenanceService);
    }

    MaintenanceService getRuntimeMaintenanceService() {
        return runtimeState.getMaintenanceService();
    }

    void setRuntimeTuning(final RuntimeTuning runtimeTuning) {
        runtimeState.setRuntimeTuning(runtimeTuning);
    }

    RuntimeTuning getRuntimeTuning() {
        return runtimeState.getRuntimeTuning();
    }

    void setRuntimeMonitoring(final IndexRuntimeMonitoring runtimeMonitoring) {
        runtimeState.setRuntimeMonitoring(runtimeMonitoring);
    }

    IndexRuntimeMonitoring getRuntimeMonitoring() {
        return runtimeState.getRuntimeMonitoring();
    }

    void markRuntimeCloseOwnershipTransferred() {
        runtimeState.markCloseOwnershipTransferred();
    }

    boolean runtimeCloseOwnershipTransferred() {
        return runtimeState.closeOwnershipTransferred();
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
