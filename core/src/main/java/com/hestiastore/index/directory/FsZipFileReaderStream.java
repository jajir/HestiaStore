package com.hestiastore.index.directory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.ZipInputStream;

import com.hestiastore.index.IndexException;

public class FsZipFileReaderStream implements FileReader {

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
            return readBytes == bytes.length ? readBytes : -1;
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
