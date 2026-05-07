package org.hestiastore.index.segmentindex.configuration.persistence;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;

/**
 * Serializes and parses chunk filter specs stored in index metadata.
 */
final class ChunkFilterSpecCodec {

    private static final String ITEM_SEPARATOR = ",";
    private static final String PART_SEPARATOR = "|";
    private static final String KEY_VALUE_SEPARATOR = "=";
    private static final String PROVIDER_PREFIX = "p=";

    private ChunkFilterSpecCodec() {
    }

    /**
     * Serializes ordered chunk filter specs into a compact metadata string.
     *
     * @param specs ordered filter specs
     * @return serialized representation stored in metadata
     */
    static String serialize(final List<ChunkFilterSpec> specs) {
        final List<ChunkFilterSpec> requiredSpecs = Vldtn.requireNonNull(specs,
                "specs");
        if (requiredSpecs.isEmpty()) {
            return "";
        }
        final List<String> encoded = new ArrayList<>(requiredSpecs.size());
        for (final ChunkFilterSpec spec : requiredSpecs) {
            encoded.add(serializeSpec(spec));
        }
        return String.join(ITEM_SEPARATOR, encoded);
    }

    /**
     * Parses chunk filter specs from persisted metadata.
     *
     * <p>
     * Both the current provider-based format and the legacy class-name format
     * are supported.
     * </p>
     *
     * @param raw raw metadata value
     * @return immutable ordered filter specs
     */
    static List<ChunkFilterSpec> parse(final String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        final String[] tokens = raw.split(ITEM_SEPARATOR);
        final List<ChunkFilterSpec> specs = new ArrayList<>(tokens.length);
        for (final String token : tokens) {
            final String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                specs.add(parseToken(trimmed));
            }
        }
        return List.copyOf(specs);
    }

    private static String serializeSpec(final ChunkFilterSpec spec) {
        final ChunkFilterSpec requiredSpec = Vldtn.requireNonNull(spec, "spec");
        final StringBuilder builder = new StringBuilder();
        builder.append(PROVIDER_PREFIX)
                .append(encode(requiredSpec.getProviderId()));
        requiredSpec.getParameters().forEach((key, value) -> builder
                .append(PART_SEPARATOR).append(encode(key))
                .append(KEY_VALUE_SEPARATOR).append(encode(value)));
        return builder.toString();
    }

    private static ChunkFilterSpec parseToken(final String token) {
        if (!token.startsWith(PROVIDER_PREFIX)) {
            return ChunkFilterSpecs.fromPersistedClassName(token);
        }
        final String[] parts = token.split("\\|");
        ChunkFilterSpec spec = ChunkFilterSpec
                .ofProvider(decode(parts[0].substring(PROVIDER_PREFIX.length())));
        for (int i = 1; i < parts.length; i++) {
            final String part = parts[i];
            final int separatorIndex = part.indexOf(KEY_VALUE_SEPARATOR);
            if (separatorIndex <= 0 || separatorIndex == part.length() - 1) {
                throw new IllegalArgumentException(String.format(
                        "Invalid chunk filter parameter token '%s'", token));
            }
            final String key = decode(part.substring(0, separatorIndex));
            final String value = decode(part.substring(separatorIndex + 1));
            spec = spec.withParameter(key, value);
        }
        return spec;
    }

    private static String encode(final String value) {
        return URLEncoder.encode(Vldtn.requireNonNull(value, "value"),
                StandardCharsets.UTF_8);
    }

    private static String decode(final String value) {
        return URLDecoder.decode(Vldtn.requireNonNull(value, "value"),
                StandardCharsets.UTF_8);
    }
}
