package com.coroptis.index.directory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.coroptis.index.IndexException;

public class MemFileWriter implements FileWriter {

    private final String fileName;

    private final ByteArrayOutputStream fio;

    private final MemDirectory memDirectory;

    final Directory.Access access;

    MemFileWriter(final String fileName, final MemDirectory memDirectory,
            final Directory.Access access) {
        this.fileName = fileName;
        this.memDirectory = memDirectory;
        this.fio = new ByteArrayOutputStream();
        this.access = access;
    }

    @Override
    public void close() {
        try {
            fio.close();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
        memDirectory.addFile(fileName, fio.toByteArray(), access);
    }

    @Override
    public void write(byte b) {
        fio.write(b);
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
