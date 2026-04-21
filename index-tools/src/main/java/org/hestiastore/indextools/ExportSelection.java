package org.hestiastore.indextools;

import java.util.stream.Stream;

import org.hestiastore.index.Entry;

final class ExportSelection {

    private final String fromKeyText;
    private final String toKeyText;
    private final Long limit;
    private final Object fromKey;
    private final Object toKey;

    private ExportSelection(final String fromKeyText, final String toKeyText,
            final Long limit, final Object fromKey, final Object toKey) {
        this.fromKeyText = fromKeyText;
        this.toKeyText = toKeyText;
        this.limit = limit;
        this.fromKey = fromKey;
        this.toKey = toKey;
    }

    static ExportSelection create(final String fromKeyText,
            final String toKeyText, final Long limit,
            final DescriptorSupport.DescriptorBinding keyBinding) {
        final TextValueCodecRegistry textCodecs = new TextValueCodecRegistry();
        final Object fromKey = fromKeyText == null ? null
                : textCodecs.fromText(fromKeyText, keyBinding.getJavaClass(),
                        keyBinding.getDescriptorClassName());
        final Object toKey = toKeyText == null ? null
                : textCodecs.fromText(toKeyText, keyBinding.getJavaClass(),
                        keyBinding.getDescriptorClassName());
        if ((fromKey != null || toKey != null)
                && !Comparable.class.isAssignableFrom(keyBinding.getJavaClass())) {
            throw new IllegalArgumentException(
                    "Range export requires comparable keys. Add a text codec and comparable key type for range filtering.");
        }
        if (limit != null && limit.longValue() < 0L) {
            throw new IllegalArgumentException(
                    "limit must be >= 0 (was " + limit + ")");
        }
        if (fromKey != null && toKey != null
                && compareKeys(fromKey, toKey) > 0) {
            throw new IllegalArgumentException(
                    "from-key must be <= to-key for range export.");
        }
        return new ExportSelection(fromKeyText, toKeyText, limit, fromKey,
                toKey);
    }

    Stream<Entry<Object, Object>> apply(
            final Stream<Entry<Object, Object>> stream) {
        Stream<Entry<Object, Object>> filtered = stream;
        if (fromKey != null) {
            filtered = filtered
                    .filter(entry -> compareKeys(entry.getKey(), fromKey) >= 0);
        }
        if (toKey != null) {
            filtered = filtered
                    .filter(entry -> compareKeys(entry.getKey(), toKey) <= 0);
        }
        if (limit != null) {
            filtered = filtered.limit(limit.longValue());
        }
        return filtered;
    }

    String getFromKeyText() {
        return fromKeyText;
    }

    String getToKeyText() {
        return toKeyText;
    }

    Long getLimit() {
        return limit;
    }

    boolean isPartial() {
        return fromKeyText != null || toKeyText != null || limit != null;
    }

    @SuppressWarnings("unchecked")
    private static int compareKeys(final Object left, final Object right) {
        return ((Comparable<Object>) left).compareTo(right);
    }
}
