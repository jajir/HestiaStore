package org.hestiastore.index.segmentindex.core.split;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.function.LongConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentHandle;

/**
 * Public runtime boundary for background split scheduling, admission, and
 * lifecycle coordination.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface BackgroundSplitCoordinator<K, V> {

    /**
     * Builds the default runtime background split coordinator from split
     * collaborators and policies.
     *
     * @param conf index configuration
     * @param keyComparator key comparator
     * @param keyToSegmentMap route map
     * @param segmentRegistry segment registry
     * @param directoryFacade root segment directory
     * @param splitExecutor split executor
     * @param splitFailureHandler failure handler
     * @param splitAppliedListener listener invoked after successful split
     * @param splitTaskStartDelayRecorder split task delay recorder
     * @param splitTaskRunLatencyRecorder split task runtime recorder
     * @param <K> key type
     * @param <V> value type
     * @return background split coordinator
     */
    static <K, V> BackgroundSplitCoordinator<K, V> create(
            final IndexConfiguration<K, V> conf,
            final Comparator<K> keyComparator,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final Directory directoryFacade,
            final Executor splitExecutor,
            final Consumer<RuntimeException> splitFailureHandler,
            final Runnable splitAppliedListener,
            final LongConsumer splitTaskStartDelayRecorder,
            final LongConsumer splitTaskRunLatencyRecorder) {
        final SegmentRegistry<K, V> validatedSegmentRegistry = Vldtn
                .requireNonNull(segmentRegistry, "segmentRegistry");
        final DefaultSegmentMaterializationService<K, V> materializationService =
                new DefaultSegmentMaterializationService<>(
                        Vldtn.requireNonNull(directoryFacade,
                                "directoryFacade"),
                        validatedSegmentRegistry.materialization());
        return new BackgroundSplitCoordinatorImpl<>(
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                new RouteSplitCoordinator<>(
                        Vldtn.requireNonNull(conf, "conf"),
                        Vldtn.requireNonNull(keyComparator, "keyComparator"),
                        validatedSegmentRegistry, materializationService),
                new RouteSplitPublishCoordinator<>(
                        keyToSegmentMap, validatedSegmentRegistry,
                        materializationService),
                Vldtn.requireNonNull(splitExecutor, "splitExecutor"),
                Vldtn.requireNonNull(splitFailureHandler,
                        "splitFailureHandler"),
                Vldtn.requireNonNull(splitAppliedListener,
                        "splitAppliedListener"),
                Vldtn.requireNonNull(splitTaskStartDelayRecorder,
                        "splitTaskStartDelayRecorder"),
                Vldtn.requireNonNull(splitTaskRunLatencyRecorder,
                        "splitTaskRunLatencyRecorder"));
    }

    /**
     * Attempts to schedule a background split for the provided segment.
     *
     * @param segmentHandle split candidate
     * @param splitThreshold minimum number of keys that makes the candidate
     *        eligible
     * @param ignoreCooldown whether failed-attempt cooldown should be bypassed
     * @return {@code true} when split work was scheduled
     */
    boolean handleSplitCandidate(SegmentHandle<K, V> segmentHandle,
            long splitThreshold, boolean ignoreCooldown);

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
     * @param <T> action result type
     * @return action result
     */
    <T> T runWithSplitSchedulingPaused(Supplier<T> action);

    /**
     * Runs the supplied action while new split scheduling is paused.
     *
     * @param action action to run
     */
    void runWithSplitSchedulingPaused(Runnable action);

    /**
     * Runs the supplied action under shared split admission against split
     * publish.
     *
     * @param action action to run
     * @param <T> action result type
     * @return action result
     */
    <T> T runWithSharedSplitAdmission(Supplier<T> action);
}
