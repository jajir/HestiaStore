package org.hestiastore.index.segmentindex.core.session;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.ResolvedIndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.metrics.Stats;
import org.slf4j.Logger;

/**
 * Validated runtime assembly input bundle.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexRuntimeInputs<K, V> {

    public final Logger logger;
    public final Directory directoryFacade;
    public final TypeDescriptor<K> keyTypeDescriptor;
    public final TypeDescriptor<V> valueTypeDescriptor;
    public final IndexConfiguration<K, V> conf;
    public final ResolvedIndexConfiguration<K, V> runtimeConfiguration;
    public final ExecutorRegistry executorRegistry;
    public final Stats stats;
    public final AtomicLong compactRequestHighWaterMark;
    public final AtomicLong flushRequestHighWaterMark;
    public final AtomicLong lastAppliedWalLsn;
    public final Supplier<SegmentIndexState> stateSupplier;
    public final Consumer<RuntimeException> failureHandler;

    public SegmentIndexRuntimeInputs(
            final Logger logger,
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final ResolvedIndexConfiguration<K, V> runtimeConfiguration,
            final ExecutorRegistry executorRegistry,
            final Stats stats,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final AtomicLong lastAppliedWalLsn,
            final Supplier<SegmentIndexState> stateSupplier,
            final Consumer<RuntimeException> failureHandler) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.runtimeConfiguration = Vldtn.requireNonNull(runtimeConfiguration,
                "runtimeConfiguration");
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.compactRequestHighWaterMark = Vldtn.requireNonNull(
                compactRequestHighWaterMark, "compactRequestHighWaterMark");
        this.flushRequestHighWaterMark = Vldtn.requireNonNull(
                flushRequestHighWaterMark, "flushRequestHighWaterMark");
        this.lastAppliedWalLsn = Vldtn.requireNonNull(lastAppliedWalLsn,
                "lastAppliedWalLsn");
        this.stateSupplier = Vldtn.requireNonNull(stateSupplier,
                "stateSupplier");
        this.failureHandler = Vldtn.requireNonNull(failureHandler,
                "failureHandler");
    }

}
