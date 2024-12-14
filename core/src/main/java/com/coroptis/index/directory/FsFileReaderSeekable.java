package com.coroptis.index.directory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.coroptis.index.IndexException;

public class FsFileReaderSeekable implements FileReaderSeekable {

    private final RandomAccessFile raf;

    FsFileReaderSeekable(final File file) {
        try {
            raf = new RandomAccessFile(file, "r");
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public int read() {
        try {
            return raf.read();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public int read(final byte[] bytes) {
        try {
            return raf.read(bytes);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void skip(final long bytesToSkip) {
        try {
            int skippedBytes = raf.skipBytes((int) bytesToSkip);
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
    public void close() {
        try {
            raf.close();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void seek(final long position) {
        try {
            raf.seek(position);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

}
