package com.coroptis.index.directory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import com.coroptis.index.IndexException;

public class FsFileReaderStream implements FileReader {

    private final BufferedInputStream bis;

    FsFileReaderStream(final File file, final int bufferSize) {
        try {
            final Path path = file.toPath();
            final InputStream fin = Files.newInputStream(path);
            bis = new BufferedInputStream(fin, bufferSize);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
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
    public int read(final byte[] bytes) {
        try {
            final int readBytes = bis.read(bytes);
            return readBytes;
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
        return String.format("FsFileReaderStream[bis='%s']", bis.toString());
    }

}
