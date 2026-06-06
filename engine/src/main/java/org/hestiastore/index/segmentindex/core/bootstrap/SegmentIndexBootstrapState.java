package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentLeaseService;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexResourceClosingAdapter;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexRuntimeServices;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionHandle;
import org.hestiastore.index.segmentindex.core.session.SegmentTopologyRuntimeAccess;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;
import org.hestiastore.index.segmentindex.logging.IndexMdcCallWrapper;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

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
    private IndexMdcCallWrapper indexMdcCallWrapper;
    // FIXME why to store x time the same?
    private SegmentIndexSessionHandle<K, V> internalIndex;
    private SegmentIndexSessionHandle<K, V> managedIndex;
    private SegmentIndexResourceClosingAdapter<K, V> index;
    private SegmentIndexBootstrapResult<K, V> result;
    private SegmentIndexCoreStorage<K, V> coreStorage;
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

    void setIndexMdcCallWrapper(
            final IndexMdcCallWrapper indexMdcCallWrapper) {
        this.indexMdcCallWrapper = Vldtn.requireNonNull(indexMdcCallWrapper,
                "indexMdcCallWrapper");
    }

    boolean hasIndexMdcCallWrapper() {
        return indexMdcCallWrapper != null;
    }

    IndexMdcCallWrapper getIndexMdcCallWrapper() {
        return requireInitialized(indexMdcCallWrapper, "indexMdcCallWrapper");
    }

    void setInternalIndex(
            final SegmentIndexSessionHandle<K, V> internalIndex) {
        this.internalIndex = Vldtn.requireNonNull(internalIndex,
                "internalIndex");
    }

    SegmentIndexSessionHandle<K, V> getInternalIndex() {
        return requireInitialized(internalIndex, "internalIndex");
    }

    void setManagedIndex(final SegmentIndexSessionHandle<K, V> managedIndex) {
        this.managedIndex = Vldtn.requireNonNull(managedIndex, "managedIndex");
    }

    SegmentIndexSessionHandle<K, V> getManagedIndex() {
        return requireInitialized(managedIndex, "managedIndex");
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

    void setCoreStorage(final SegmentIndexCoreStorage<K, V> coreStorage) {
        this.coreStorage = Vldtn.requireNonNull(coreStorage, "coreStorage");
    }

    boolean hasCoreStorage() {
        return coreStorage != null;
    }

    SegmentIndexCoreStorage<K, V> getCoreStorage() {
        return requireInitialized(coreStorage, "coreStorage");
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
