package org.hestiastore.index.control.model;

import org.hestiastore.index.Vldtn;

/**
 * Runtime patch apply result.
 */
public final class RuntimePatchResult {

    private final boolean applied;
    private final RuntimePatchValidation validation;
    private final ConfigurationSnapshot snapshot;

    /**
     * Creates validated patch result.
     */
    public RuntimePatchResult(final boolean applied,
            final RuntimePatchValidation validation,
            final ConfigurationSnapshot snapshot) {
        this.validation = Vldtn.requireNonNull(validation, "validation");
        this.snapshot = Vldtn.requireNonNull(snapshot, "snapshot");
        Vldtn.requireTrue(!applied || validation.valid(),
                "applied=true requires valid validation");
        this.applied = applied;
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

    // Backward-compatible accessor style for existing call sites.
    public boolean applied() {
        return applied;
    }

    // Backward-compatible accessor style for existing call sites.
    public RuntimePatchValidation validation() {
        return validation;
    }

    // Backward-compatible accessor style for existing call sites.
    public ConfigurationSnapshot snapshot() {
        return snapshot;
    }
}
