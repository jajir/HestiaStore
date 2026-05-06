package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.ResolvedIndexConfiguration;
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
    private final IndexConfiguration<K, V> conf;
    private final ResolvedIndexConfiguration<K, V> resolvedConfiguration;
    private final ExecutorRegistry executorRegistry;

    public SegmentIndexCoreStorageOpenSpec(
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final ResolvedIndexConfiguration<K, V> resolvedConfiguration,
            final ExecutorRegistry executorRegistry) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.resolvedConfiguration = Vldtn.requireNonNull(resolvedConfiguration,
                "resolvedConfiguration");
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

    public IndexConfiguration<K, V> conf() {
        return conf;
    }

    public ResolvedIndexConfiguration<K, V> resolvedConfiguration() {
        return resolvedConfiguration;
    }

    public ExecutorRegistry executorRegistry() {
        return executorRegistry;
    }
}
