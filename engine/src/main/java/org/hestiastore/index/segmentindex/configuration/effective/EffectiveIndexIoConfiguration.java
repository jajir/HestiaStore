package org.hestiastore.index.segmentindex.configuration.effective;

import org.hestiastore.index.Vldtn;

/**
 * Resolved disk I/O settings.
 */
public final class EffectiveIndexIoConfiguration {

    private final int diskBufferSizeBytes;

    public EffectiveIndexIoConfiguration(final int diskBufferSizeBytes) {
        this.diskBufferSizeBytes = Vldtn.requireIoBufferSize(
                diskBufferSizeBytes);
    }

    public int diskBufferSizeBytes() {
        return diskBufferSizeBytes;
    }
}
