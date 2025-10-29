package org.hestiastore.index.directory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.ZipInputStream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.MutableBytes;
import org.hestiastore.index.Vldtn;

public final class FsZipFileReaderStream extends AbstractCloseableResource
        implements FileReader {

    private final ZipInputStream bis;

    FsZipFileReaderStream(final File file) {
        try {
            FileInputStream fin = new FileInputStream(file);
            bis = new ZipInputStream(new BufferedInputStream(fin, 1024 * 10));
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    protected void doClose() {
        try {
            bis.close();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public int read() {
        try {
            return bis.read();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public int read(final MutableBytes bytes) {
        final byte[] data = Vldtn.requireNonNull(bytes, "bytes").array();
        try {
            final int readBytes = bis.read(data, 0, bytes.length());
            return readBytes == bytes.length() ? readBytes : -1;
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void skip(final long bytesToSkip) {
        try {
            long skippedBytes = bis.skip(bytesToSkip);
            if (skippedBytes != bytesToSkip) {
                throw new IndexException(String.format(
                        "In file should be '%s' bytes skipped but "
                                + "actually was skipped '%s' bytes.",
                        bytesToSkip, skippedBytes));
            }

        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return String.format("FsZipFileReaderStream[bis='%s']", bis.toString());
    }

}
