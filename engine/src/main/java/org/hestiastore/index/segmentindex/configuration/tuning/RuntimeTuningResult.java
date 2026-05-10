package org.hestiastore.index.segmentindex.configuration.tuning;

import java.util.List;

import org.hestiastore.index.Vldtn;

/**
 * Runtime tuning patch apply result.
 */
public final class RuntimeTuningResult {

    private final RuntimeTuningApplyStatus status;
    private final RuntimeTuningSnapshot before;
    private final RuntimeTuningSnapshot after;
    private final RuntimeTuningValidation validation;
    private final List<RuntimeTuningChange> changes;

    RuntimeTuningResult(final RuntimeTuningApplyStatus status,
            final RuntimeTuningSnapshot before,
            final RuntimeTuningSnapshot after,
            final RuntimeTuningValidation validation,
            final List<RuntimeTuningChange> changes) {
        this.status = Vldtn.requireNonNull(status, "status");
        this.before = Vldtn.requireNonNull(before, "before");
        this.after = Vldtn.requireNonNull(after, "after");
        this.validation = Vldtn.requireNonNull(validation, "validation");
        this.changes = List.copyOf(Vldtn.requireNonNull(changes, "changes"));
        Vldtn.requireTrue(
                status != RuntimeTuningApplyStatus.APPLIED
                        || validation.valid(),
                "APPLIED requires valid validation");
        Vldtn.requireTrue(
                status != RuntimeTuningApplyStatus.REJECTED
                        || !validation.valid(),
                "REJECTED requires invalid validation");
        Vldtn.requireTrue(
                status != RuntimeTuningApplyStatus.REJECTED
                        || this.changes.isEmpty(),
                "REJECTED requires empty changes");
    }

    public RuntimeTuningApplyStatus status() {
        return status;
    }

    public boolean applied() {
        return status == RuntimeTuningApplyStatus.APPLIED;
    }

    public RuntimeTuningSnapshot before() {
        return before;
    }

    public RuntimeTuningSnapshot after() {
        return after;
    }

    public RuntimeTuningValidation validation() {
        return validation;
    }

    public List<RuntimeTuningChange> changes() {
        return changes;
    }
}
