package org.hestiastore.index.segmentindex.core.routing;

import java.util.function.Supplier;

import org.hestiastore.index.segment.SegmentId;

/**
 * Routed-operation view of split admission and split blocking.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SplitAdmissionAccess<K, V> {

    /**
     * Runs an action under shared split admission against split publish.
     *
     * @param action action to run
     * @param <T> result type
     * @return action result
     */
    <T> T runWithSharedSplitAdmission(Supplier<T> action);

    /**
     * @param segmentId segment id
     * @return {@code true} when the segment currently has active split work
     */
    boolean isSplitBlocked(SegmentId segmentId);
}
