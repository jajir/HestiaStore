package org.hestiastore.index.segmentindex.core.split;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Unified split-management boundary that owns both split execution and split
 * policy scheduling.
 *
 * @param <K> key type
 * @param <V> value type
 */
interface BackgroundSplitRuntimeAccess<K, V>
        extends SplitService<K, V>, BackgroundSplitCoordinator<K, V> {

    /**
     * Builds the default split runtime boundary with both execution and policy
     * collaborators wired together.
     *
     * @param conf index configuration
     * @param runtimeTuningState runtime tuning state
     * @param keyComparator key comparator
     * @param keyToSegmentMap route map
     * @param segmentRegistry segment registry
     * @param directoryFacade root segment directory
     * @param splitExecutor split executor
     * @param workerExecutor split-policy worker executor
     * @param splitPolicyScheduler split-policy scheduler
     * @param lifecycle host runtime lifecycle view
     * @param failureReporter split failure reporter
     * @param telemetry split telemetry recorder
     * @param <K> key type
     * @param <V> value type
     * @return unified split-management runtime boundary
     */
    static <K, V> BackgroundSplitRuntimeAccess<K, V> create(
            final IndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final Comparator<K> keyComparator,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final Directory directoryFacade,
            final Executor splitExecutor, final Executor workerExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final SplitRuntimeLifecycle lifecycle,
            final SplitFailureReporter failureReporter,
            final SplitRuntimeTelemetry telemetry) {
        return new SplitServiceImpl<>(
                Vldtn.requireNonNull(conf, "conf"),
                Vldtn.requireNonNull(runtimeTuningState,
                        "runtimeTuningState"),
                Vldtn.requireNonNull(keyComparator, "keyComparator"),
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(segmentRegistry, "segmentRegistry"),
                Vldtn.requireNonNull(directoryFacade, "directoryFacade"),
                Vldtn.requireNonNull(splitExecutor, "splitExecutor"),
                Vldtn.requireNonNull(workerExecutor, "workerExecutor"),
                Vldtn.requireNonNull(splitPolicyScheduler,
                        "splitPolicyScheduler"),
                Vldtn.requireNonNull(lifecycle, "lifecycle"),
                Vldtn.requireNonNull(failureReporter, "failureReporter"),
                Vldtn.requireNonNull(telemetry, "telemetry"),
                new BackgroundSplitPolicyWorkState());
    }

    /**
     * Requests a full policy scan over currently mapped segments.
     */
    void scheduleScan();

    /**
     * Requests a full policy scan when no split is currently in flight.
     */
    void scheduleScanIfIdle();

    /**
     * Waits until the split runtime has exhausted policy work and in-flight
     * splits.
     */
    void awaitExhausted();
}
