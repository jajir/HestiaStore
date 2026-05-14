package org.hestiastore.index.segmentindex.core.session;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitStatsRecorder;

/**
 * Validated context for opening the runtime graph.
 *
 * @param <K> key type
 * @param <V> value type
 */
// FIXME it should not be used at all
final class SegmentIndexRuntimeOpenContext<K, V> {

    final Directory directoryFacade;
    final TypeDescriptor<K> keyTypeDescriptor;
    final TypeDescriptor<V> valueTypeDescriptor;
    final EffectiveIndexConfiguration<K, V> conf;
    final ExecutorRegistry executorRegistry;
    final IndexOperationStatsRecorder operationStatsRecorder;
    final MaintenanceStatsRecorder maintenanceStatsRecorder;
    final SplitStatsRecorder splitStatsRecorder;
    final AtomicLong compactRequestHighWaterMark;
    final AtomicLong flushRequestHighWaterMark;
    final AtomicLong lastAppliedWalLsn;
    final Supplier<SegmentIndexState> stateSupplier;
    final Consumer<RuntimeException> failureHandler;

    SegmentIndexRuntimeOpenContext(
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final EffectiveIndexConfiguration<K, V> conf,
            final ExecutorRegistry executorRegistry,
            final IndexOperationStatsRecorder operationStatsRecorder,
            final MaintenanceStatsRecorder maintenanceStatsRecorder,
            final SplitStatsRecorder splitStatsRecorder,
            final AtomicLong compactRequestHighWaterMark,
            final AtomicLong flushRequestHighWaterMark,
            final AtomicLong lastAppliedWalLsn,
            final Supplier<SegmentIndexState> stateSupplier,
            final Consumer<RuntimeException> failureHandler) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
        this.operationStatsRecorder = Vldtn.requireNonNull(
                operationStatsRecorder, "operationStatsRecorder");
        this.maintenanceStatsRecorder = Vldtn.requireNonNull(
                maintenanceStatsRecorder, "maintenanceStatsRecorder");
        this.splitStatsRecorder = Vldtn.requireNonNull(splitStatsRecorder,
                "splitStatsRecorder");
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
