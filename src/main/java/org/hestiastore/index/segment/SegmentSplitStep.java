package org.hestiastore.index.segment;

/**
 * A single step in the segment split pipeline. Steps can either
 * produce a final {@link SegmentSplitterResult} or return {@code null}
 * to delegate to the next step.
 */
interface SegmentSplitStep<K, V> {
    /**
     * Executes this split step using the provided context and mutable state.
     * <p>
     * A step should either produce a final {@link SegmentSplitterResult} or
     * return {@code null} to indicate that the pipeline should continue with
     * the next step. Steps may update the supplied {@code state} to pass data
     * to subsequent steps (e.g., opened iterator, created lower segment).
     *
     * @param ctx   immutable inputs for the current split execution
     * @param state mutable state shared across steps in the pipeline
     * @return a non-null result to terminate the pipeline, or {@code null} to
     *         continue with the next step
     * @throws RuntimeException if the step detects an unrecoverable condition
     *                          (validation failure, IO error, etc.)
     */
    SegmentSplitterResult<K, V> perform(SegmentSplitContext<K, V> ctx,
            SegmentSplitState<K, V> state);
}
