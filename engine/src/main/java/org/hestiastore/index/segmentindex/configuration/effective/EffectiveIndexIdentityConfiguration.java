package org.hestiastore.index.segmentindex.configuration.effective;

import org.hestiastore.index.Vldtn;

/**
 * Resolved index identity and type metadata.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class EffectiveIndexIdentityConfiguration<K, V> {

    private final String indexName;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final String keyTypeDescriptor;
    private final String valueTypeDescriptor;

    public EffectiveIndexIdentityConfiguration(final String indexName,
            final Class<K> keyClass, final Class<V> valueClass,
            final String keyTypeDescriptor,
            final String valueTypeDescriptor) {
        this.indexName = Vldtn.requireNotBlank(indexName, "indexName");
        this.keyClass = Vldtn.requireNonNull(keyClass, "keyClass");
        this.valueClass = Vldtn.requireNonNull(valueClass, "valueClass");
        this.keyTypeDescriptor = Vldtn.requireNotBlank(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNotBlank(valueTypeDescriptor,
                "valueTypeDescriptor");
    }

    public String name() {
        return indexName;
    }

    public Class<K> keyClass() {
        return keyClass;
    }

    public Class<V> valueClass() {
        return valueClass;
    }

    public String keyTypeDescriptor() {
        return keyTypeDescriptor;
    }

    public String valueTypeDescriptor() {
        return valueTypeDescriptor;
    }
}
