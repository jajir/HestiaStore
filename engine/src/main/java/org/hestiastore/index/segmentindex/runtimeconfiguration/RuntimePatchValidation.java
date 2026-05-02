package org.hestiastore.index.segmentindex.runtimeconfiguration;

import java.util.List;
import java.util.Map;

import org.hestiastore.index.Vldtn;

/**
 * Validation result for runtime patch.
 */
public record RuntimePatchValidation(boolean valid,
        List<ValidationIssue> issues,
        Map<RuntimeSettingKey, Integer> normalizedValues) {

    public RuntimePatchValidation {
        final List<ValidationIssue> issueList = Vldtn.requireNonNull(issues,
                "issues");
        final Map<RuntimeSettingKey, Integer> values = Vldtn.requireNonNull(
                normalizedValues, "normalizedValues");
        Vldtn.requireTrue(!valid || issueList.isEmpty(),
                "valid=true requires empty issues");
        Vldtn.requireTrue(valid || !issueList.isEmpty(),
                "valid=false requires at least one issue");
        issues = List.copyOf(issueList);
        normalizedValues = Map.copyOf(values);
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
}
