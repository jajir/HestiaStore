package org.hestiastore.index.control.model;

import org.hestiastore.index.Vldtn;

/**
 * One validation issue found while checking runtime patch values.
 */
public final class ValidationIssue {

    private final RuntimeSettingKey key;
    private final String message;

    /**
     * Creates validated issue.
     */
    public ValidationIssue(final RuntimeSettingKey key, final String message) {
        this.key = key;
        this.message = Vldtn.requireNotBlank(message, "message");
    }

    public RuntimeSettingKey getKey() {
        return key;
    }

    public String getMessage() {
        return message;
    }

    // Backward-compatible accessor style for existing call sites.
    public RuntimeSettingKey key() {
        return key;
    }

    // Backward-compatible accessor style for existing call sites.
    public String message() {
        return message;
    }
}
