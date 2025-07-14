package org.hestiastore.index.datatype;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.FileWriter;

public class ByteArrayWriter implements FileWriter {

    private final ByteArrayOutputStream fio;

    ByteArrayWriter() {
        this.fio = new ByteArrayOutputStream();
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

    byte[] toByteArray() {
        return fio.toByteArray();
    }

}
