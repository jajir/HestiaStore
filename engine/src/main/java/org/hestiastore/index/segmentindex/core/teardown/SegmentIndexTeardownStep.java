package org.hestiastore.index.segmentindex.core.teardown;

/**
 * One close step in the segment-index teardown pipeline.
 *
 * @param <C> teardown context type
 */
public interface SegmentIndexTeardownStep<C> {

    /**
     * Applies this close step.
     *
     * @param context typed access to the closing index collaborators
     */
    void apply(C context);
}
