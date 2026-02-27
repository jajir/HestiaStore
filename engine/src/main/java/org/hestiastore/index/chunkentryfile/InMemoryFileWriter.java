package org.hestiastore.index.chunkentryfile;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.bytes.BytesAppender;
import org.hestiastore.index.directory.FileWriter;

/**
 * Simple in-memory FileWriter backed by BytesAppender.
 */
class InMemoryFileWriter extends AbstractCloseableResource
        implements FileWriter {
    private static final ByteSequence[] ONE_BYTE_SEQUENCES = buildOneByteSequences();

    private final BytesAppender appender;
    private boolean closed = false;

    InMemoryFileWriter(final BytesAppender appender) {
        this.appender = Vldtn.requireNonNull(appender, "appender");
    }

    @Override
    public void write(final byte b) {
        ensureOpen();
        appender.append(ONE_BYTE_SEQUENCES[b & 0xFF]);
    }

    @Override
    public void write(final byte[] bytes) {
        write(bytes, 0, Vldtn.requireNonNull(bytes, "bytes").length);
    }

    @Override
    public void write(final byte[] bytes, final int offset, final int length) {
        final byte[] validated = Vldtn.requireNonNull(bytes, "bytes");
        final int from = Vldtn.requireGreaterThanOrEqualToZero(offset,
                "offset");
        final int len = Vldtn.requireGreaterThanOrEqualToZero(length, "length");
        if (from > validated.length || from + len > validated.length) {
            throw new IllegalArgumentException(String.format(
                    "Offset '%s' and length '%s' exceed source length '%s'",
                    from, len, validated.length));
        }
        ensureOpen();
        if (len == 0) {
            return;
        }
        appender.append(ByteSequences
                .copyOf(ByteSequences.viewOf(validated, from, from + len)));
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

    private static ByteSequence[] buildOneByteSequences() {
        final ByteSequence[] out = new ByteSequence[256];
        for (int i = 0; i < out.length; i++) {
            out[i] = ByteSequences.wrap(new byte[] { (byte) i });
        }
        return out;
    }
}
