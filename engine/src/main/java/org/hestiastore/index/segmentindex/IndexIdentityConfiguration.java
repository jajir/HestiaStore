package org.hestiastore.index.segmentindex;

/**
 * Immutable index identity and key/value type metadata view.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexIdentityConfiguration<K, V> {

    private final String indexName;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final String keyTypeDescriptor;
    private final String valueTypeDescriptor;

    IndexIdentityConfiguration(final String indexName,
            final Class<K> keyClass, final Class<V> valueClass,
            final String keyTypeDescriptor,
            final String valueTypeDescriptor) {
        this.indexName = indexName;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.keyTypeDescriptor = keyTypeDescriptor;
        this.valueTypeDescriptor = valueTypeDescriptor;
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
