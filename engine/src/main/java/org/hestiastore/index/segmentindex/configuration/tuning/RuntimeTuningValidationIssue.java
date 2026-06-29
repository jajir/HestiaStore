package org.hestiastore.index.segmentindex.configuration.tuning;

import org.hestiastore.index.Vldtn;

/**
 * One validation issue found while checking runtime tuning values.
 */
public final class RuntimeTuningValidationIssue {

    private final RuntimeTuningKey field;
    private final String message;

    public RuntimeTuningValidationIssue(final RuntimeTuningKey field,
            final String message) {
        this.field = field;
        this.message = Vldtn.requireNotBlank(message, "message");
    }

    public RuntimeTuningKey field() {
        return field;
    }

    public String message() {
        return message;
    }
}
