package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.IndexMdcScope;

/**
 * Runs segment-index bootstrap steps and rolls back completed steps after a
 * bootstrap failure or a no-index early result.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexBootstrapPipeline<K, V> {

    private final List<SegmentIndexBootstrapStep<K, V>> steps;

    SegmentIndexBootstrapPipeline(
            final List<SegmentIndexBootstrapStep<K, V>> steps) {
        this.steps = List.copyOf(Vldtn.requireNonNull(steps, "steps"));
    }

    SegmentIndexBootstrapResult<K, V> run(
            final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        final SegmentIndexBootstrapRequest<K, V> nonNullRequest = Vldtn
                .requireNonNull(request, "request");
        final SegmentIndexBootstrapState<K, V> nonNullState = Vldtn
                .requireNonNull(state, "state");
        final List<SegmentIndexBootstrapStep<K, V>> appliedSteps = new ArrayList<>();
        boolean noIndexResultCleanupStarted = false;
        try {
            for (final SegmentIndexBootstrapStep<K, V> step : steps) {
                appliedSteps.add(step);
                runInScope(nonNullState,
                        () -> step.apply(nonNullRequest, nonNullState));
                if (nonNullState.hasResult()) {
                    final SegmentIndexBootstrapResult<K, V> result =
                            nonNullState.getResult();
                    noIndexResultCleanupStarted = result.index().isEmpty();
                    closeAppliedStepsAfterNoIndexResult(nonNullState,
                            appliedSteps, result);
                    return result;
                }
            }
            return resultFromState(nonNullRequest, nonNullState);
        } catch (final RuntimeException failure) {
            if (!noIndexResultCleanupStarted) {
                closeAppliedSteps(nonNullState, appliedSteps, failure);
            }
            throw failure;
        }
    }

    private SegmentIndexBootstrapResult<K, V> resultFromState(
            final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        return switch (request.getMode()) {
            case CREATE -> SegmentIndexBootstrapResult.created(state.getIndex());
            case OPEN, TRY_OPEN -> SegmentIndexBootstrapResult.opened(
                    state.getIndex());
        };
    }

    private void closeAppliedSteps(
            final SegmentIndexBootstrapState<K, V> state,
            final List<SegmentIndexBootstrapStep<K, V>> appliedSteps,
            final RuntimeException failure) {
        for (int i = appliedSteps.size() - 1; i >= 0; i--) {
            final SegmentIndexBootstrapStep<K, V> step = appliedSteps.get(i);
            try {
                runInScope(state, step::closeResource);
            } catch (final RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
    }

    private void closeAppliedStepsAfterNoIndexResult(
            final SegmentIndexBootstrapState<K, V> state,
            final List<SegmentIndexBootstrapStep<K, V>> appliedSteps,
            final SegmentIndexBootstrapResult<K, V> result) {
        if (result.index().isPresent()) {
            return;
        }
        RuntimeException firstCleanupFailure = null;
        for (int i = appliedSteps.size() - 1; i >= 0; i--) {
            final SegmentIndexBootstrapStep<K, V> step = appliedSteps.get(i);
            try {
                runInScope(state, step::closeResource);
            } catch (final RuntimeException cleanupFailure) {
                if (firstCleanupFailure == null) {
                    firstCleanupFailure = cleanupFailure;
                } else {
                    firstCleanupFailure.addSuppressed(cleanupFailure);
                }
            }
        }
        if (firstCleanupFailure != null) {
            throw firstCleanupFailure;
        }
    }

    private void runInScope(final SegmentIndexBootstrapState<K, V> state,
            final Runnable action) {
        if (!state.hasIndexMdcScopeRunner()) {
            action.run();
            return;
        }
        try (IndexMdcScope ignored = state.getIndexMdcScopeRunner()
                .openScope()) {
            action.run();
        }
    }
}
