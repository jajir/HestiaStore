package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;

public class DataBlockReaderImpl implements DataBlockReader {

    private final FileReader fileReader;
    private final int blockSize;
    private int position;

    DataBlockReaderImpl(final FileReader fileReader,
            final DataBlockPosition blockPosition, final int blockSize) {
        this.fileReader = Vldtn.requireNonNull(fileReader, "fileReader");
        this.blockSize = blockSize;
        this.position = blockPosition.getValue();
    }

    @Override
    public void close() {
        fileReader.close();
    }

    @Override
    public DataBlock read() {
        final byte[] buffer = new byte[blockSize];
        final int bytesRead = fileReader.read(buffer);
        if (bytesRead < 0) {
            return null; // End of file reached
        }
        if (bytesRead != blockSize) {
            throw new IndexException("Unable to read full block");
        }
        DataBlockPosition blockPosition = DataBlockPosition.of(position);
        position += blockSize;
        return new DataBlock(Bytes.of(buffer), blockPosition);
    }

}
