package org.hestiastore.index.directory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory.Access;

public final class FsNioFileWriterStream extends AbstractCloseableResource
        implements FileWriter {

    private final FileChannel channel;
    private final byte[] singleByte = new byte[1];

    public FsNioFileWriterStream(final File file, final Access access) {
        try {
            if (access == Access.OVERWRITE) {
                channel = FileChannel.open(file.toPath(),
                        StandardOpenOption.CREATE, // Create file if it doesn't
                                                   // exist
                        StandardOpenOption.TRUNCATE_EXISTING, // If it exists,
                                                              // truncate it
                        StandardOpenOption.WRITE // Open for writing
                );
            } else {
                channel = FileChannel.open(file.toPath(),
                        StandardOpenOption.CREATE, // Create file if it doesn't
                                                   // exist
                        StandardOpenOption.APPEND, // If it exists, append to it
                        StandardOpenOption.WRITE // Open for writing
                );
            }
        } catch (IOException e) {
            throw new IndexException("Error opening file channel for writing",
                    e);
        }
    }

    @Override
    public void write(final byte b) {
        singleByte[0] = b;
        writeInternal(singleByte, 0, 1);
    }

    private void writeInternal(final byte[] data, final int offset,
            final int length) {
        final ByteBuffer buffer = ByteBuffer.wrap(data, offset, length);
        try {
            channel.write(buffer);
        } catch (IOException e) {
            throw new IndexException("Error writing to file channel", e);
        }
    }

    @Override
    public void write(final Bytes bytes) {
        final byte[] data = Vldtn.requireNonNull(bytes, "bytes").getData();
        writeInternal(data, 0, data.length);
    }

    public void flush() {
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new IndexException("Error flushing file channel", e);
        }
    }

    @Override
    protected void doClose() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new IndexException("Error closing file channel", e);
        }
    }
}
