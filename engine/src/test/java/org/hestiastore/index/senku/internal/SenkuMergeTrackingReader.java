package org.hestiastore.index.senku.internal;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.MemFileReader;

/** Seekable in-memory reader that can fail only after marking itself closed. */
final class SenkuMergeTrackingReader extends MemFileReader
        implements FileReaderSeekable {
    private final IndexException closeFailure;

    SenkuMergeTrackingReader(final ByteSequence bytes,
            final IndexException closeFailure) {
        super(bytes);
        this.closeFailure = closeFailure;
    }

    @Override
    public void seek(final long position) {
        if (position < 0 || position > getDataLength()) {
            throw new IllegalArgumentException("Invalid fixture seek position");
        }
        setPosition(position);
    }

    @Override
    protected void doClose() {
        super.doClose();
        if (closeFailure != null) {
            throw closeFailure;
        }
    }
}
