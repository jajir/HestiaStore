package org.hestiastore.index.segmentindex.split;

import java.util.function.Supplier;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * Public runtime boundary for background split scheduling, admission, and
 * lifecycle coordination.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface BackgroundSplitCoordinator<K, V> {

    /**
     * Attempts to schedule a background split for the provided segment.
     *
     * @param segment split candidate
     * @param splitThreshold minimum number of keys that makes the candidate
     *        eligible
     * @param ignoreCooldown whether failed-attempt cooldown should be bypassed
     * @return {@code true} when split work was scheduled
     */
    boolean handleSplitCandidate(Segment<K, V> segment, long splitThreshold,
            boolean ignoreCooldown);

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
