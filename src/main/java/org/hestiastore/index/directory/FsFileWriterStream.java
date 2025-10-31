package org.hestiastore.index.directory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory.Access;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FsFileWriterStream extends AbstractCloseableResource
        implements FileWriter {

    private static final Logger logger = LoggerFactory
            .getLogger(FsFileWriterStream.class);

    private final OutputStream fio;

    private static final int BUFFER_SIZE = 1024 * 1 * 4;

    FsFileWriterStream(final File file, final Directory.Access access) {
        this(file, access, BUFFER_SIZE);
    }

    FsFileWriterStream(final File file, final Directory.Access access,
            final int bufferSize) {
        try {
            final Path path = file.toPath();
            if (access == Access.OVERWRITE && file.exists() && !file.delete()) {
                logger.warn("Unable to delete file '{}'", file.getName());
            }
            final OutputStream os = Files.newOutputStream(path,
                    Directory.Access.APPEND == access
                            ? StandardOpenOption.APPEND
                            : StandardOpenOption.CREATE);
            this.fio = new BufferedOutputStream(os, bufferSize);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    protected void doClose() {
        try {
            fio.close();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void write(byte b) {
        try {
            fio.write(b);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void write(final ByteSequence bytes) {
        final byte[] data = Vldtn.requireNonNull(bytes, "bytes").toByteArray();
        try {
            fio.write(data);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

}
