package org.hestiastore.index.datatype;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

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

    @Override
    public void write(byte b) {
        fio.write(b);
    }

    @Override
    public void write(final Bytes bytes) {
        final byte[] data = Vldtn.requireNonNull(bytes, "bytes").getData();
        try {
            fio.write(data);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    byte[] toByteArray() {
        return fio.toByteArray();
    }

    Bytes toBytes() {
        return Bytes.of(fio.toByteArray());
    }

}
