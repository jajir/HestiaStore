package org.hestiastore.management.api;

import java.util.Map;

import java.util.Objects;

/**
 * Request payload for runtime-safe configuration updates.
 *
 * @param values key/value pairs to update
 * @param dryRun when true only validate and do not apply changes
 */
public record ConfigPatchRequest(Map<String, String> values, boolean dryRun) {

    /**
     * Creates validated config patch request.
     *
     * @param values patch values
     * @param dryRun validation-only flag
     */
    public ConfigPatchRequest {
        final Map<String, String> input = Objects.requireNonNull(values, "values");
        if (input.isEmpty()) {
            throw new IllegalArgumentException("values must not be empty");
        }
        for (Map.Entry<String, String> entry : input.entrySet()) {
            final String key = Objects.requireNonNull(entry.getKey(), "key")
                    .trim();
            final String value = Objects.requireNonNull(entry.getValue(), "value")
                    .trim();
            if (key.isEmpty()) {
                throw new IllegalArgumentException("config key must not be blank");
            }
            if (value.isEmpty()) {
                throw new IllegalArgumentException(
                        "config value must not be blank");
            }
        }
        values = Map.copyOf(input);
    }
}
