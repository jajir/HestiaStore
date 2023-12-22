package com.coroptis.index.directory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import com.coroptis.index.IndexException;

public class FsFileWriterStream implements FileWriter {

    private final OutputStream fio;

    private static final int BUFFER_SIZE = 1024 * 1 * 4;

    FsFileWriterStream(final File file, final Directory.Access access) {
        try {
            final Path path = file.toPath();
            if (file.exists()) {
                file.delete();
            }
            final OutputStream os = Files.newOutputStream(path,
                    Directory.Access.APPEND == access
                            ? StandardOpenOption.APPEND
                            : StandardOpenOption.CREATE);
            this.fio = new BufferedOutputStream(os, BUFFER_SIZE);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
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
    public void write(byte[] bytes) {
        try {
            fio.write(bytes);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

}
