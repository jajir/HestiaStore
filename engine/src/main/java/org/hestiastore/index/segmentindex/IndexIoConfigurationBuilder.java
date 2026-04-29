package org.hestiastore.index.segmentindex;

/**
 * Builder section for disk I/O settings.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexIoConfigurationBuilder<K, V> {

    private Integer diskBufferSizeBytes;

    IndexIoConfigurationBuilder() {
    }

    /**
     * Sets disk I/O buffer size in bytes.
     *
     * @param value disk I/O buffer size
     * @return this section builder
     */
    public IndexIoConfigurationBuilder<K, V> diskBufferSizeBytes(
            final Integer value) {
        this.diskBufferSizeBytes = value;
        return this;
    }

    IndexIoConfiguration build() {
        return new IndexIoConfiguration(diskBufferSizeBytes);
    }
}
