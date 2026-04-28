package org.hestiastore.index.segmentindex;

/**
 * Immutable disk I/O settings view.
 */
public final class IndexIoConfiguration {

    private final Integer diskBufferSizeBytes;

    public IndexIoConfiguration(final Integer diskBufferSizeBytes) {
        this.diskBufferSizeBytes = diskBufferSizeBytes;
    }

    public Integer diskBufferSizeBytes() {
        return diskBufferSizeBytes;
    }
}
