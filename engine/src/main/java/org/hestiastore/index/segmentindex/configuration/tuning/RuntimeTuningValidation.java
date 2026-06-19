package org.hestiastore.index.segmentindex.configuration.tuning;

import java.util.List;
import java.util.Map;

import org.hestiastore.index.Vldtn;

/**
 * Validation result for runtime tuning patches.
 */
public final class RuntimeTuningValidation {

    private final boolean valid;
    private final List<RuntimeTuningValidationIssue> issues;
    private final Map<RuntimeTuningKey, RuntimeTuningValue> normalizedValues;

    RuntimeTuningValidation(final boolean valid,
            final List<RuntimeTuningValidationIssue> issues,
            final Map<RuntimeTuningKey, RuntimeTuningValue> normalizedValues) {
        final List<RuntimeTuningValidationIssue> issueList =
                Vldtn.requireNonNull(issues, "issues");
        final Map<RuntimeTuningKey, RuntimeTuningValue> values = Vldtn
                .requireNonNull(normalizedValues, "normalizedValues");
        Vldtn.requireTrue(!valid || issueList.isEmpty(),
                "valid=true requires empty issues");
        Vldtn.requireTrue(valid || !issueList.isEmpty(),
                "valid=false requires at least one issue");
        this.valid = valid;
        this.issues = List.copyOf(issueList);
        this.normalizedValues = Map.copyOf(values);
    }

    public boolean valid() {
        return valid;
    }

    public List<RuntimeTuningValidationIssue> issues() {
        return issues;
    }

    Map<RuntimeTuningKey, RuntimeTuningValue> normalizedValues() {
        return normalizedValues;
    }
}
