package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;

/**
 * Validated input bundle for opening core storage collaborators.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexCoreStorageOpenSpec<K, V> {

    private final Directory directoryFacade;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final EffectiveIndexConfiguration<K, V> conf;
    private final ExecutorRegistry executorRegistry;

    public SegmentIndexCoreStorageOpenSpec(
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final EffectiveIndexConfiguration<K, V> conf,
            final ExecutorRegistry executorRegistry) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
    }

    public Directory directoryFacade() {
        return directoryFacade;
    }

    public TypeDescriptor<K> keyTypeDescriptor() {
        return keyTypeDescriptor;
    }

    public TypeDescriptor<V> valueTypeDescriptor() {
        return valueTypeDescriptor;
    }

    public EffectiveIndexConfiguration<K, V> conf() {
        return conf;
    }

    public ExecutorRegistry executorRegistry() {
        return executorRegistry;
    }
}
