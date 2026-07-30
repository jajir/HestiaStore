package org.hestiastore.index.segmentindex.configuration.api;

/**
 * Builder section for disk I/O settings.
 */
public final class IndexIoConfigurationBuilder {

    private Integer diskBufferSizeBytes;

    IndexIoConfigurationBuilder() {
    }

    /**
     * Sets disk I/O buffer size in bytes.
     *
     * @param value disk I/O buffer size
     * @return this section builder
     */
    public IndexIoConfigurationBuilder diskBufferSizeBytes(
            final Integer value) {
        this.diskBufferSizeBytes = value;
        return this;
    }

    IndexIoConfiguration build() {
        return new IndexIoConfiguration(diskBufferSizeBytes);
    }
}
