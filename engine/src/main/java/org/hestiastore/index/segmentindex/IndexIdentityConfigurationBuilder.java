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

    private final IndexConfigurationBuilder<K, V> builder;

    IndexIdentityConfigurationBuilder(
            final IndexConfigurationBuilder<K, V> builder) {
        this.builder = Vldtn.requireNonNull(builder, "builder");
    }

    /**
     * Sets logical index name.
     *
     * @param value index name
     * @return this section builder
     */
    public IndexIdentityConfigurationBuilder<K, V> name(final String value) {
        builder.setName(value);
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
        builder.setKeyClass(value);
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
        builder.setValueClass(value);
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
        builder.setKeyTypeDescriptor(value);
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
        builder.setKeyTypeDescriptor(value);
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
        builder.setValueTypeDescriptor(value);
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
        builder.setValueTypeDescriptor(value);
        return this;
    }
}
