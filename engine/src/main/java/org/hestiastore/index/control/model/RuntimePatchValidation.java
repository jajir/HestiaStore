package org.hestiastore.index.control.model;

import java.util.List;
import java.util.Map;

import org.hestiastore.index.Vldtn;

/**
 * Validation result for runtime patch.
 */
public final class RuntimePatchValidation {

    private final boolean valid;
    private final List<ValidationIssue> issues;
    private final Map<RuntimeSettingKey, Integer> normalizedValues;

    /**
     * Creates validated patch validation result.
     */
    public RuntimePatchValidation(final boolean valid,
            final List<ValidationIssue> issues,
            final Map<RuntimeSettingKey, Integer> normalizedValues) {
        final List<ValidationIssue> issueList = Vldtn.requireNonNull(issues,
                "issues");
        final Map<RuntimeSettingKey, Integer> values = Vldtn.requireNonNull(
                normalizedValues, "normalizedValues");
        Vldtn.requireTrue(!valid || issueList.isEmpty(),
                "valid=true requires empty issues");
        Vldtn.requireTrue(valid || !issueList.isEmpty(),
                "valid=false requires at least one issue");
        this.valid = valid;
        this.issues = List.copyOf(issueList);
        this.normalizedValues = Map.copyOf(values);
    }

    public boolean isValid() {
        return valid;
    }

    public List<ValidationIssue> getIssues() {
        return issues;
    }

    public Map<RuntimeSettingKey, Integer> getNormalizedValues() {
        return normalizedValues;
    }

    // Backward-compatible accessor style for existing call sites.
    public boolean valid() {
        return valid;
    }

    // Backward-compatible accessor style for existing call sites.
    public List<ValidationIssue> issues() {
        return issues;
    }

    // Backward-compatible accessor style for existing call sites.
    public Map<RuntimeSettingKey, Integer> normalizedValues() {
        return normalizedValues;
    }
}
