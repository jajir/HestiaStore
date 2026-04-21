package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.core.infrastructure.IndexExecutorRegistry;
import org.hestiastore.index.segmentindex.core.state.IndexStateCoordinator;
import org.slf4j.Logger;

/**
 * Validated input bundle for assembling one segment-index core graph.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexCoreInputs<K, V> {

    final Logger logger;
    final Directory directoryFacade;
    final TypeDescriptor<K> keyTypeDescriptor;
    final TypeDescriptor<V> valueTypeDescriptor;
    final IndexConfiguration<K, V> conf;
    final IndexRuntimeConfiguration<K, V> runtimeConfiguration;
    final IndexExecutorRegistry executorRegistry;
    final IndexStateCoordinator<K, V> stateCoordinator;
    final boolean staleLockRecovered;

    private SegmentIndexCoreInputs(final Logger logger,
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final IndexRuntimeConfiguration<K, V> runtimeConfiguration,
            final IndexExecutorRegistry executorRegistry,
            final IndexStateCoordinator<K, V> stateCoordinator,
            final boolean staleLockRecovered) {
        this.logger = logger;
        this.directoryFacade = directoryFacade;
        this.keyTypeDescriptor = keyTypeDescriptor;
        this.valueTypeDescriptor = valueTypeDescriptor;
        this.conf = conf;
        this.runtimeConfiguration = runtimeConfiguration;
        this.executorRegistry = executorRegistry;
        this.stateCoordinator = stateCoordinator;
        this.staleLockRecovered = staleLockRecovered;
    }

    static <K, V> SegmentIndexCoreInputs<K, V> create(
            final Logger logger, final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final IndexRuntimeConfiguration<K, V> runtimeConfiguration,
            final IndexExecutorRegistry executorRegistry,
            final IndexStateCoordinator<K, V> stateCoordinator,
            final boolean staleLockRecovered) {
        return new SegmentIndexCoreInputs<>(
                Vldtn.requireNonNull(logger, "logger"),
                Vldtn.requireNonNull(directoryFacade, "directoryFacade"),
                Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor"),
                Vldtn.requireNonNull(valueTypeDescriptor,
                        "valueTypeDescriptor"),
                Vldtn.requireNonNull(conf, "conf"),
                Vldtn.requireNonNull(runtimeConfiguration,
                        "runtimeConfiguration"),
                Vldtn.requireNonNull(executorRegistry, "executorRegistry"),
                Vldtn.requireNonNull(stateCoordinator, "stateCoordinator"),
                staleLockRecovered);
    }
}
