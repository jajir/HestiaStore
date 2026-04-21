package org.hestiastore.indextools;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

final class CountingOutputStream extends FilterOutputStream {

    private long writtenBytes;

    CountingOutputStream(final OutputStream outputStream) {
        super(outputStream);
    }

    @Override
    public void write(final int value) throws IOException {
        out.write(value);
        writtenBytes++;
    }

    @Override
    public void write(final byte[] buffer, final int offset, final int length)
            throws IOException {
        out.write(buffer, offset, length);
        writtenBytes += length;
    }

    long getWrittenBytes() {
        return writtenBytes;
    }
}
