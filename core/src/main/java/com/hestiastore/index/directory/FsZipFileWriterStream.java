package com.hestiastore.index.directory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.hestiastore.index.IndexException;

public class FsZipFileWriterStream implements FileWriter {

    private final ZipOutputStream fio;

    private final static int BUFFER_SIZE = 1024 * 100;

    FsZipFileWriterStream(final File file) {
        try {
            this.fio = new ZipOutputStream(new BufferedOutputStream(
                    new FileOutputStream(file), BUFFER_SIZE));
            fio.setMethod(ZipOutputStream.DEFLATED);
            fio.setLevel(9);
            fio.putNextEntry(new ZipEntry("default.dat"));
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            fio.closeEntry();
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
