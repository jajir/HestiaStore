package org.hestiastore.index.segmentindex.core.split;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.maintenance.SplitMaintenanceSynchronization;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.routing.SplitAdmissionAccess;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Builds the default split runtime and its local routing/maintenance adapters.
 */
public final class SplitRuntimeFactory {

    private SplitRuntimeFactory() {
    }

    /**
     * Creates the default split runtime service.
     *
     * @param conf index configuration
     * @param runtimeTuningState runtime tuning state
     * @param keyComparator key comparator
     * @param keyToSegmentMap route map
     * @param segmentRegistry segment registry
     * @param directoryFacade root segment directory
     * @param splitExecutor split executor
     * @param workerExecutor policy worker executor
     * @param splitPolicyScheduler policy scheduler
     * @param stateSupplier host runtime state supplier
     * @param failureHandler fatal split failure handler
     * @param stats split telemetry source
     * @param <K> key type
     * @param <V> value type
     * @return split service
     */
    public static <K, V> SplitService<K, V> create(
            final IndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final Comparator<K> keyComparator,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final Directory directoryFacade,
            final Executor splitExecutor, final Executor workerExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final Supplier<SegmentIndexState> stateSupplier,
            final Consumer<RuntimeException> failureHandler,
            final Stats stats) {
        return SplitService.<K, V>builder()
                .conf(Vldtn.requireNonNull(conf, "conf"))
                .runtimeTuningState(Vldtn.requireNonNull(runtimeTuningState,
                        "runtimeTuningState"))
                .keyComparator(Vldtn.requireNonNull(keyComparator,
                        "keyComparator"))
                .keyToSegmentMap(Vldtn.requireNonNull(keyToSegmentMap,
                        "keyToSegmentMap"))
                .segmentRegistry(Vldtn.requireNonNull(segmentRegistry,
                        "segmentRegistry"))
                .directoryFacade(Vldtn.requireNonNull(directoryFacade,
                        "directoryFacade"))
                .splitExecutor(Vldtn.requireNonNull(splitExecutor,
                        "splitExecutor"))
                .workerExecutor(Vldtn.requireNonNull(workerExecutor,
                        "workerExecutor"))
                .splitPolicyScheduler(Vldtn.requireNonNull(
                        splitPolicyScheduler, "splitPolicyScheduler"))
                .stateSupplier(Vldtn.requireNonNull(stateSupplier,
                        "stateSupplier"))
                .failureHandler(Vldtn.requireNonNull(failureHandler,
                        "failureHandler"))
                .stats(Vldtn.requireNonNull(stats, "stats"))
                .build();
    }

    /**
     * Creates the routing-facing split admission adapter for the split
     * service.
     *
     * @param splitService split service instance created by this factory
     * @param <K> key type
     * @param <V> value type
     * @return routing-facing split admission adapter
     */
    public static <K, V> SplitAdmissionAccess<K, V> admissionAccess(
            final SplitService<K, V> splitService) {
        return Vldtn.requireNonNull(splitService, "splitService")
                .splitAdmission();
    }

    /**
     * Creates the maintenance-facing synchronization adapter for the split
     * service.
     *
     * @param splitService split service instance created by this factory
     * @param <K> key type
     * @param <V> value type
     * @return maintenance-facing synchronization adapter
     */
    public static <K, V> SplitMaintenanceSynchronization<K, V> maintenanceSynchronization(
            final SplitService<K, V> splitService) {
        return Vldtn.requireNonNull(splitService, "splitService")
                .splitMaintenance();
    }

    /**
     * Requests an internal reconciliation pass.
     *
     * @param splitService split service instance created by this factory
     * @param <K> key type
     * @param <V> value type
     */
    public static <K, V> void requestReconciliation(
            final SplitService<K, V> splitService) {
        Vldtn.requireNonNull(splitService, "splitService")
                .splitMaintenance()
                .requestReconciliation();
    }

    /**
     * Returns the immutable runtime snapshot exposed by the default split
     * runtime.
     *
     * @param splitService split service instance created by this factory
     * @param <K> key type
     * @param <V> value type
     * @return split runtime snapshot
     */
    public static <K, V> SplitMetricsSnapshot metricsSnapshot(
            final SplitService<K, V> splitService) {
        return Vldtn.requireNonNull(splitService, "splitService")
                .splitMetricsView()
                .metricsSnapshot();
    }
}
