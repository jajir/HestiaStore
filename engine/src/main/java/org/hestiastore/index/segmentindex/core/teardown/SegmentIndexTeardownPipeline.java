package org.hestiastore.index.segmentindex.core.teardown;

import java.util.List;

import org.hestiastore.index.Vldtn;

/**
 * Runs segment-index teardown steps and preserves later close failures as
 * suppressed exceptions on the first failure.
 *
 * @param <C> teardown context type
 */
public final class SegmentIndexTeardownPipeline<C> {

    private final List<SegmentIndexTeardownStep<C>> closeSteps;

    SegmentIndexTeardownPipeline(
            final List<SegmentIndexTeardownStep<C>> closeSteps) {
        this.closeSteps = List.copyOf(Vldtn.requireNonNull(closeSteps,
                "closeSteps"));
    }

    /**
     * Creates a segment-index teardown pipeline.
     *
     * @param <C>        teardown context type
     * @param closeSteps close steps
     * @return close teardown pipeline
     */
    public static <C> SegmentIndexTeardownPipeline<C> of(
            final List<SegmentIndexTeardownStep<C>> closeSteps) {
        return new SegmentIndexTeardownPipeline<>(closeSteps);
    }

    /**
     * Runs every configured close step.
     *
     * @param context typed access to the closing index collaborators
     */
    public void run(final C context) {
        final C nonNullContext = Vldtn.requireNonNull(context, "context");
        RuntimeException firstFailure = null;
        for (final SegmentIndexTeardownStep<C> closeStep : closeSteps) {
            try {
                closeStep.apply(nonNullContext);
            } catch (final RuntimeException failure) {
                firstFailure = recordFailure(firstFailure, failure);
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private static RuntimeException recordFailure(
            final RuntimeException firstFailure,
            final RuntimeException failure) {
        if (firstFailure == null) {
            return failure;
        }
        firstFailure.addSuppressed(failure);
        return firstFailure;
    }
}
