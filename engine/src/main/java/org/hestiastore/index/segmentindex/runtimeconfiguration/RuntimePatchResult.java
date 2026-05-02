package org.hestiastore.index.segmentindex.runtimeconfiguration;

import org.hestiastore.index.Vldtn;

/**
 * Runtime patch apply result.
 */
public record RuntimePatchResult(boolean applied,
        RuntimePatchValidation validation, ConfigurationSnapshot snapshot) {

    public RuntimePatchResult {
        validation = Vldtn.requireNonNull(validation, "validation");
        snapshot = Vldtn.requireNonNull(snapshot, "snapshot");
        Vldtn.requireTrue(!applied || validation.valid(),
                "applied=true requires valid validation");
    }

    public boolean isApplied() {
        return applied;
    }

    public RuntimePatchValidation getValidation() {
        return validation;
    }

    public ConfigurationSnapshot getSnapshot() {
        return snapshot;
    }
}
