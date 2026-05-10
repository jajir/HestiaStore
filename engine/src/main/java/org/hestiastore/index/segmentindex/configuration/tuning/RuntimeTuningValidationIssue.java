package org.hestiastore.index.segmentindex.configuration.tuning;

import org.hestiastore.index.Vldtn;

/**
 * One validation issue found while checking runtime tuning values.
 */
public final class RuntimeTuningValidationIssue {

    private final RuntimeTuningField field;
    private final String message;

    public RuntimeTuningValidationIssue(final RuntimeTuningField field,
            final String message) {
        this.field = field;
        this.message = Vldtn.requireNotBlank(message, "message");
    }

    public RuntimeTuningField field() {
        return field;
    }

    public String message() {
        return message;
    }
}
