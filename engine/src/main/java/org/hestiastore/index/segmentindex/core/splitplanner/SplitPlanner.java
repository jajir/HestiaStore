package org.hestiastore.index.segmentindex.core.splitplanner;

import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.routing.BackgroundSplitCoordinator;

/**
 * Control-plane admission point for split planning requests.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SplitPlanner<K, V> {

    static <K, V> SplitPlanner<K, V> create(
            final IndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final SplitTaskDispatcher<K, V> splitTaskDispatcher,
            final Executor plannerExecutor,
            final Supplier<SegmentIndexState> stateSupplier,
            final Runnable awaitSplitsIdleAction,
            final Consumer<RuntimeException> failureHandler) {
        return new SplitPlannerLoop<>(
                Vldtn.requireNonNull(conf, "conf"),
                Vldtn.requireNonNull(runtimeTuningState, "runtimeTuningState"),
                Vldtn.requireNonNull(backgroundSplitCoordinator,
                        "backgroundSplitCoordinator"),
                Vldtn.requireNonNull(splitTaskDispatcher,
                        "splitTaskDispatcher"),
                Vldtn.requireNonNull(plannerExecutor, "plannerExecutor"),
                Vldtn.requireNonNull(stateSupplier, "stateSupplier"),
                Vldtn.requireNonNull(awaitSplitsIdleAction,
                        "awaitSplitsIdleAction"),
                Vldtn.requireNonNull(failureHandler, "failureHandler"),
                new SplitPlannerState());
    }

    void hintSegment(SegmentId segmentId);

    void requestRescan();

    void scheduleIfIdle();

    void awaitExhausted();
}
