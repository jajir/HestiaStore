package org.hestiastore.index.control.model;

import org.hestiastore.index.Vldtn;

/**
 * One validation issue found while checking runtime patch values.
 */
public record ValidationIssue(RuntimeSettingKey key, String message) {

    public ValidationIssue {
        message = Vldtn.requireNotBlank(message, "message");
    }

    public RuntimeSettingKey getKey() {
        return key;
    }

    public String getMessage() {
        return message;
    }
}
