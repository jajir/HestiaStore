package org.hestiastore.index.segmentindex.core.split;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentHandle;

/**
 * Coordinates split execution and topology-aware split publishing after the
 * policy layer has already accepted a candidate.
 *
 * @param <K> key type
 * @param <V> value type
 */
interface SplitExecutionCoordinator<K, V> {

    /**
     * Builds the default split-execution coordinator from split collaborators
     * and policies.
     *
     * @param conf            index configuration
     * @param keyComparator   key comparator
     * @param keyToSegmentMap route map
     * @param segmentRegistry segment registry
     * @param segmentTopology runtime route topology
     * @param directoryFacade root segment directory
     * @param splitExecutor   split executor
     * @param failureReporter split failure reporter
     * @param onSplitApplied  callback invoked after a split is successfully
     *                        applied
     * @param telemetry       split telemetry recorder
     * @param <K>             key type
     * @param <V>             value type
     * @return split-execution coordinator
     */
    static <K, V> SplitExecutionCoordinator<K, V> create(
            final IndexConfiguration<K, V> conf,
            final Comparator<K> keyComparator,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentTopology<K> segmentTopology,
            final Directory directoryFacade,
            final Executor splitExecutor,
            final SplitFailureReporter failureReporter,
            final Runnable onSplitApplied, final SplitTelemetry telemetry) {
        final SegmentRegistry<K, V> validatedSegmentRegistry = Vldtn
                .requireNonNull(segmentRegistry, "segmentRegistry");
        final DefaultSegmentMaterializationService<K, V> materializationService = new DefaultSegmentMaterializationService<>(
                Vldtn.requireNonNull(directoryFacade,
                        "directoryFacade"),
                validatedSegmentRegistry.materialization());
        return new SplitExecutionCoordinatorImpl<>(
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(segmentTopology, "segmentTopology"),
                new RouteSplitCoordinator<>(
                        Vldtn.requireNonNull(conf, "conf"),
                        Vldtn.requireNonNull(keyComparator, "keyComparator"),
                        validatedSegmentRegistry, materializationService),
                new RouteSplitPublishCoordinator<>(
                        keyToSegmentMap, validatedSegmentRegistry,
                        materializationService),
                Vldtn.requireNonNull(splitExecutor, "splitExecutor"),
                Vldtn.requireNonNull(failureReporter, "failureReporter"),
                Vldtn.requireNonNull(onSplitApplied, "onSplitApplied"),
                Vldtn.requireNonNull(telemetry, "telemetry"));
    }

    /**
     * Schedules split execution for a candidate already accepted by the policy
     * layer.
     *
     * @param segmentHandle    accepted split candidate
     * @param splitThreshold   active split threshold
     * @param observedKeyCount key count observed by policy evaluation
     * @return {@code true} when split work was scheduled
     */
    boolean scheduleEligibleSplit(SegmentHandle<K, V> segmentHandle,
            long splitThreshold, long observedKeyCount);

    /**
     * Waits until in-flight splits finish or the timeout expires.
     *
     * @param timeoutMillis wait timeout in milliseconds
     */
    void awaitSplitsIdle(long timeoutMillis);

    /**
     * @return number of scheduled or running split tasks
     */
    int splitInFlightCount();

    /**
     * @param segmentId segment id
     * @return {@code true} when the segment currently has a scheduled or
     *         running split
     */
    boolean isSplitBlocked(SegmentId segmentId);

    /**
     * @return number of blocked segments with scheduled or running splits
     */
    int splitBlockedCount();

    /**
     * Runs the supplied action while new split scheduling is paused.
     *
     * @param action action to run
     * @param <T>    action result type
     * @return action result
     */
    <T> T runWithSplitSchedulingPaused(Supplier<T> action);

    /**
     * Runs the supplied action while new split scheduling is paused.
     *
     * @param action action to run
     */
    void runWithSplitSchedulingPaused(Runnable action);

}
