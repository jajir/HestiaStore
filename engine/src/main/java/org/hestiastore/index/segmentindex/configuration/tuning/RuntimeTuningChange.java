package org.hestiastore.index.segmentindex.configuration.tuning;

import org.hestiastore.index.Vldtn;

/**
 * One effective runtime tuning value change.
 */
public final class RuntimeTuningChange {

    private final RuntimeTuningField field;
    private final RuntimeTuningValue before;
    private final RuntimeTuningValue after;

    public RuntimeTuningChange(final RuntimeTuningField field,
            final RuntimeTuningValue before, final RuntimeTuningValue after) {
        this.field = Vldtn.requireNonNull(field, "field");
        this.before = Vldtn.requireNonNull(before, "before");
        this.after = Vldtn.requireNonNull(after, "after");
        Vldtn.requireTrue(!this.before.equals(this.after),
                "before and after must differ");
    }

    public RuntimeTuningField field() {
        return field;
    }

    public RuntimeTuningValue before() {
        return before;
    }

    public RuntimeTuningValue after() {
        return after;
    }
}
