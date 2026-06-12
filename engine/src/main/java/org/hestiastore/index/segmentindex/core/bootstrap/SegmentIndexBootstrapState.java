package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentLeaseService;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexResourceClosingAdapter;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexRuntimeServices;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResource;
import org.hestiastore.index.segmentindex.core.session.SegmentTopologyRuntimeAccess;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
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
    private SegmentIndexSessionResource<K, V> indexHandle;
    private SegmentIndexResourceClosingAdapter<K, V> index;
    private SegmentIndexBootstrapResult<K, V> result;
    private RuntimeTuningState runtimeTuningState;
    private KeyToSegmentMap<K> keyToSegmentMap;
    private ChunkStoreCache<K, V> chunkStoreCache;
    private SegmentRegistry<K, V> segmentRegistry;
    private StorageService<K, V> storageService;
    private SegmentLeaseService<K, V> runtimeSegmentLeaseService;
    private SplitService runtimeSplitService;
    private SegmentTopologyRuntimeAccess<K, V> runtimeTopologyRuntime;
    private WalRuntime<K, V> runtimeWalRuntime;
    private SegmentIndexRuntimeServices<K, V> runtimeServices;
    private Boolean indexRuntimeWasCreated = Boolean.FALSE;

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

    void setCoreStorageRuntime(
            final CoreStorageRuntime<K, V> coreStorageRuntime) {
        final CoreStorageRuntime<K, V> validatedCoreStorageRuntime =
                Vldtn.requireNonNull(coreStorageRuntime,
                        "coreStorageRuntime");
        runtimeTuningState =
                validatedCoreStorageRuntime.getRuntimeTuningState();
        storageService = validatedCoreStorageRuntime.getStorageService();
    }

    boolean hasCoreStorage() {
        return storageService != null;
    }

    RuntimeTuningState getRuntimeTuningState() {
        return requireInitialized(runtimeTuningState, "runtimeTuningState");
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
        return requireInitialized(storageService, "storageService");
    }

    /**
     * Closes opened core storage resources during failed bootstrap before the
     * runtime takes ownership.
     */
    void closeCoreStorage() {
        RuntimeException failure = null;
        failure = closeSegmentRegistry(failure);
        failure = closeKeyToSegmentMap(failure);
        if (failure != null) {
            throw failure;
        }
    }

    private RuntimeException closeSegmentRegistry(
            final RuntimeException failure) {
        if (segmentRegistry == null) {
            return failure;
        }
        try {
            segmentRegistry.close();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException closeKeyToSegmentMap(
            final RuntimeException failure) {
        if (keyToSegmentMap == null || keyToSegmentMap.wasClosed()) {
            return failure;
        }
        try {
            keyToSegmentMap.close();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException appendCleanupFailure(
            final RuntimeException failure,
            final RuntimeException cleanupFailure) {
        if (failure == null) {
            return cleanupFailure;
        }
        failure.addSuppressed(cleanupFailure);
        return failure;
    }

    void setRuntimeSegmentLeaseService(
            final SegmentLeaseService<K, V> runtimeSegmentLeaseService) {
        this.runtimeSegmentLeaseService = Vldtn.requireNonNull(
                runtimeSegmentLeaseService, "runtimeSegmentLeaseService");
    }

    SegmentLeaseService<K, V> getRuntimeSegmentLeaseService() {
        return requireInitialized(runtimeSegmentLeaseService,
                "runtimeSegmentLeaseService");
    }

    void setRuntimeSplitService(final SplitService runtimeSplitService) {
        this.runtimeSplitService = Vldtn.requireNonNull(runtimeSplitService,
                "runtimeSplitService");
    }

    boolean hasRuntimeSplitService() {
        return runtimeSplitService != null;
    }

    SplitService getRuntimeSplitService() {
        return requireInitialized(runtimeSplitService, "runtimeSplitService");
    }

    void setRuntimeTopologyRuntime(
            final SegmentTopologyRuntimeAccess<K, V> runtimeTopologyRuntime) {
        this.runtimeTopologyRuntime = Vldtn.requireNonNull(
                runtimeTopologyRuntime, "runtimeTopologyRuntime");
    }

    SegmentTopologyRuntimeAccess<K, V> getRuntimeTopologyRuntime() {
        return requireInitialized(runtimeTopologyRuntime,
                "runtimeTopologyRuntime");
    }

    void setRuntimeWalRuntime(final WalRuntime<K, V> runtimeWalRuntime) {
        this.runtimeWalRuntime = Vldtn.requireNonNull(runtimeWalRuntime,
                "runtimeWalRuntime");
    }

    boolean hasRuntimeWalRuntime() {
        return runtimeWalRuntime != null;
    }

    WalRuntime<K, V> getRuntimeWalRuntime() {
        return requireInitialized(runtimeWalRuntime, "runtimeWalRuntime");
    }

    void setRuntimeServices(
            final SegmentIndexRuntimeServices<K, V> runtimeServices) {
        this.runtimeServices = Vldtn.requireNonNull(runtimeServices,
                "runtimeServices");
    }

    SegmentIndexRuntimeServices<K, V> getRuntimeServices() {
        return requireInitialized(runtimeServices, "runtimeServices");
    }

    void markIndexRuntimeCreated() {
        indexRuntimeWasCreated = Boolean.TRUE;
    }

    boolean indexRuntimeWasCreated() {
        return Boolean.TRUE.equals(indexRuntimeWasCreated);
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
