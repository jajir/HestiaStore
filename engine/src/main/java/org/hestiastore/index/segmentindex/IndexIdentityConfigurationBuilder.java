package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;

/**
 * Builder section for index identity and key/value type metadata.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexIdentityConfigurationBuilder<K, V> {

    private String indexName;
    private Class<K> keyClass;
    private Class<V> valueClass;
    private String keyTypeDescriptor;
    private String valueTypeDescriptor;

    IndexIdentityConfigurationBuilder() {
    }

    /**
     * Sets logical index name.
     *
     * @param value index name
     * @return this section builder
     */
    public IndexIdentityConfigurationBuilder<K, V> name(final String value) {
        this.indexName = value;
        return this;
    }

    /**
     * Sets key class.
     *
     * @param value key class
     * @return this section builder
     */
    public IndexIdentityConfigurationBuilder<K, V> keyClass(
            final Class<K> value) {
        this.keyClass = value;
        return this;
    }

    /**
     * Sets value class.
     *
     * @param value value class
     * @return this section builder
     */
    public IndexIdentityConfigurationBuilder<K, V> valueClass(
            final Class<V> value) {
        this.valueClass = value;
        return this;
    }

    /**
     * Sets key type descriptor instance.
     *
     * @param value key type descriptor
     * @return this section builder
     */
    public IndexIdentityConfigurationBuilder<K, V> keyTypeDescriptor(
            final TypeDescriptor<K> value) {
        this.keyTypeDescriptor = Vldtn
                .requireNonNull(value, "keyTypeDescriptor")
                .getClass().getName();
        return this;
    }

    /**
     * Sets key type descriptor class name.
     *
     * @param value key type descriptor class name
     * @return this section builder
     */
    public IndexIdentityConfigurationBuilder<K, V> keyTypeDescriptor(
            final String value) {
        this.keyTypeDescriptor = value;
        return this;
    }

    /**
     * Sets value type descriptor instance.
     *
     * @param value value type descriptor
     * @return this section builder
     */
    public IndexIdentityConfigurationBuilder<K, V> valueTypeDescriptor(
            final TypeDescriptor<V> value) {
        this.valueTypeDescriptor = Vldtn
                .requireNonNull(value, "valueTypeDescriptor")
                .getClass().getName();
        return this;
    }

    /**
     * Sets value type descriptor class name.
     *
     * @param value value type descriptor class name
     * @return this section builder
     */
    public IndexIdentityConfigurationBuilder<K, V> valueTypeDescriptor(
            final String value) {
        this.valueTypeDescriptor = value;
        return this;
    }

    IndexIdentityConfiguration<K, V> build() {
        return new IndexIdentityConfiguration<>(indexName, keyClass, valueClass,
                keyTypeDescriptor, valueTypeDescriptor);
    }
}
