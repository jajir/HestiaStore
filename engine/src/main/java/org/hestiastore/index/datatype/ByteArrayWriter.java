package org.hestiastore.index.datatype;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.FileWriter;

/**
 * In-memory {@link FileWriter} implementation backed by
 * {@link ByteArrayOutputStream}.
 */
public class ByteArrayWriter extends AbstractCloseableResource
        implements FileWriter {

    private final ByteArrayOutputStream fio;

    ByteArrayWriter() {
        this.fio = new ByteArrayOutputStream();
    }

    @Override
    protected void doClose() {
        try {
            fio.close();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    /**
     * Writes one byte.
     *
     * @param b byte to append
     */
    @Override
    public void write(byte b) {
        fio.write(b);
    }

    /**
     * Writes all bytes from the provided array.
     *
     * @param bytes bytes to append
     */
    @Override
    public void write(byte[] bytes) {
        try {
            fio.write(bytes);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    /**
     * Returns all written bytes as a new array.
     *
     * @return accumulated bytes
     */
    byte[] toByteArray() {
        return fio.toByteArray();
    }

}
