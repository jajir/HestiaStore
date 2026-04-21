package org.hestiastore.index.segmentindex.core.maintenance;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.observability.Stats;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.core.split.BackgroundSplitCoordinator;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Background split-policy capability view used outside the maintenance
 * package.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface BackgroundSplitPolicyAccess<K, V> {

    static <K, V> BackgroundSplitPolicyAccess<K, V> create(
            final IndexConfiguration<K, V> conf,
            final RuntimeTuningState runtimeTuningState,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final Executor workerExecutor,
            final ScheduledExecutorService splitPolicyScheduler,
            final Stats stats,
            final Supplier<SegmentIndexState> stateSupplier,
            final Runnable awaitSplitsIdleAction,
            final Consumer<RuntimeException> failureHandler) {
        return new BackgroundSplitPolicyLoop<>(
                Vldtn.requireNonNull(conf, "conf"),
                Vldtn.requireNonNull(runtimeTuningState, "runtimeTuningState"),
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(segmentRegistry, "segmentRegistry"),
                Vldtn.requireNonNull(backgroundSplitCoordinator,
                        "backgroundSplitCoordinator"),
                Vldtn.requireNonNull(workerExecutor, "workerExecutor"),
                Vldtn.requireNonNull(splitPolicyScheduler,
                        "splitPolicyScheduler"),
                Vldtn.requireNonNull(stats, "stats"),
                Vldtn.requireNonNull(stateSupplier, "stateSupplier"),
                Vldtn.requireNonNull(awaitSplitsIdleAction,
                        "awaitSplitsIdleAction"),
                Vldtn.requireNonNull(failureHandler, "failureHandler"),
                new BackgroundSplitPolicyWorkState());
    }

    void scheduleScan();

    void scheduleScanIfIdle();

    void awaitExhausted();
}
