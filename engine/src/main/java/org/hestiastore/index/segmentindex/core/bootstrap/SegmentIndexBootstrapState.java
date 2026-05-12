package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.IndexMdcScopeRunner;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.session.IndexInternal;

/**
 * Mutable products created during one segment-index bootstrap run.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexBootstrapState<K, V> {

    private EffectiveIndexConfiguration<K, V> configuration;
    private Boolean configurationWriteRequired;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private ExecutorRegistry executorRegistry;
    private IndexMdcScopeRunner indexMdcScopeRunner;
    private IndexInternal<K, V> internalIndex;
    private SegmentIndex<K, V> managedIndex;
    private SegmentIndex<K, V> index;
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

    void setIndexMdcScopeRunner(
            final IndexMdcScopeRunner indexMdcScopeRunner) {
        this.indexMdcScopeRunner = Vldtn.requireNonNull(indexMdcScopeRunner,
                "indexMdcScopeRunner");
    }

    boolean hasIndexMdcScopeRunner() {
        return indexMdcScopeRunner != null;
    }

    IndexMdcScopeRunner getIndexMdcScopeRunner() {
        return requireInitialized(indexMdcScopeRunner, "indexMdcScopeRunner");
    }

    void setInternalIndex(
            final IndexInternal<K, V> internalIndex) {
        this.internalIndex = Vldtn.requireNonNull(internalIndex,
                "internalIndex");
    }

    IndexInternal<K, V> getInternalIndex() {
        return requireInitialized(internalIndex, "internalIndex");
    }

    void setManagedIndex(final SegmentIndex<K, V> managedIndex) {
        this.managedIndex = Vldtn.requireNonNull(managedIndex, "managedIndex");
    }

    SegmentIndex<K, V> getManagedIndex() {
        return requireInitialized(managedIndex, "managedIndex");
    }

    void setIndex(final SegmentIndex<K, V> index) {
        this.index = Vldtn.requireNonNull(index, "index");
    }

    SegmentIndex<K, V> getIndex() {
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

    private static <T> T requireInitialized(final T value,
            final String fieldName) {
        if (value == null) {
            throw new IllegalStateException(
                    fieldName + " was not initialized.");
        }
        return value;
    }
}
