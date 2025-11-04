package org.hestiastore.index.chunkentryfile;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.BytesAppender;
import org.hestiastore.index.directory.FileWriter;

/**
 * Simple in-memory FileWriter backed by BytesAppender.
 */
class InMemoryFileWriter extends AbstractCloseableResource implements FileWriter {
    private final BytesAppender appender;
    private boolean closed = false;

    InMemoryFileWriter(final BytesAppender appender) {
        this.appender = Vldtn.requireNonNull(appender, "appender");
    }

    @Override
    public void write(final byte b) {
        ensureOpen();
        appender.append(new byte[] { b });
    }

    @Override
    public void write(final byte[] bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        ensureOpen();
        appender.append(bytes);
    }

    @Override
    protected void doClose() {
        closed = true;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("FileWriter already closed");
        }
    }
}

