package org.hestiastore.index.chunkstore;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.hestiastore.index.Vldtn;

/**
 * Persisted descriptor of a configured chunk filter pipeline step.
 *
 * <p>
 * The descriptor stores only stable metadata required to reconstruct a runtime
 * filter through a {@link ChunkFilterProvider}. Runtime objects such as
 * suppliers, secrets, or DI-managed services are intentionally excluded.
 * </p>
 */
public final class ChunkFilterSpec {

    private final String providerId;
    private final Map<String, String> parameters;

    private ChunkFilterSpec(final String providerId,
            final Map<String, String> parameters) {
        this.providerId = Vldtn.requireNotBlank(providerId, "providerId");
        this.parameters = Collections
                .unmodifiableMap(new LinkedHashMap<>(Vldtn
                        .requireNonNull(parameters, "parameters")));
    }

    /**
     * Creates a filter spec with the given provider id and no parameters.
     *
     * @param providerId stable provider identifier
     * @return new filter spec
     */
    public static ChunkFilterSpec ofProvider(final String providerId) {
        return new ChunkFilterSpec(providerId, Map.of());
    }

    /**
     * Returns a copy of this spec extended with the provided parameter.
     *
     * @param key parameter key
     * @param value parameter value
     * @return new spec instance with the extra parameter
     */
    public ChunkFilterSpec withParameter(final String key,
            final String value) {
        final LinkedHashMap<String, String> next = new LinkedHashMap<>(
                parameters);
        next.put(Vldtn.requireNotBlank(key, "key"),
                Vldtn.requireNotBlank(value, "value"));
        return new ChunkFilterSpec(providerId, next);
    }

    /**
     * Returns the provider id.
     *
     * @return stable provider identifier
     */
    public String getProviderId() {
        return providerId;
    }

    /**
     * Returns immutable filter parameters.
     *
     * @return immutable parameter map
     */
    public Map<String, String> getParameters() {
        return parameters;
    }

    /**
     * Returns a parameter value or {@code null} if absent.
     *
     * @param key parameter key
     * @return parameter value or {@code null}
     */
    public String getParameter(final String key) {
        return parameters.get(Vldtn.requireNotBlank(key, "key"));
    }

    /**
     * Returns a required parameter value.
     *
     * @param key parameter key
     * @return parameter value
     */
    public String getRequiredParameter(final String key) {
        final String requiredKey = Vldtn.requireNotBlank(key, "key");
        final String value = parameters.get(requiredKey);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(String.format(
                    "Missing required chunk filter parameter '%s' for provider '%s'",
                    requiredKey, providerId));
        }
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(parameters, providerId);
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ChunkFilterSpec other)) {
            return false;
        }
        return Objects.equals(providerId, other.providerId)
                && Objects.equals(parameters, other.parameters);
    }

    @Override
    public String toString() {
        if (parameters.isEmpty()) {
            return providerId;
        }
        return providerId + parameters;
    }
}
