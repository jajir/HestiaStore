package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;

/**
 * Builder section for disk I/O settings.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexIoConfigurationBuilder<K, V> {

    private final IndexConfigurationBuilder<K, V> builder;

    IndexIoConfigurationBuilder(final IndexConfigurationBuilder<K, V> builder) {
        this.builder = Vldtn.requireNonNull(builder, "builder");
    }

    /**
     * Sets disk I/O buffer size in bytes.
     *
     * @param value disk I/O buffer size
     * @return this section builder
     */
    public IndexIoConfigurationBuilder<K, V> diskBufferSizeBytes(
            final Integer value) {
        builder.setDiskIoBufferSizeBytes(value);
        return this;
    }
}
