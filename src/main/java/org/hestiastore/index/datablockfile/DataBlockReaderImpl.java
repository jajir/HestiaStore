package org.hestiastore.index.datablockfile;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReaderSeekable;

/**
 * Implementation of {@link DataBlockReader}.
 */
public class DataBlockReaderImpl extends AbstractCloseableResource
        implements DataBlockReader {

    private final FileReaderSeekable fileReader;
    private final DataBlockSize blockSize;
    private final boolean closeReaderOnClose;
    private int position;

    DataBlockReaderImpl(final FileReaderSeekable fileReader,
            final DataBlockPosition blockPosition,
            final DataBlockSize blockSize,
            final boolean closeReaderOnClose) {
        this.fileReader = Vldtn.requireNonNull(fileReader, "fileReader");
        this.blockSize = blockSize;
        this.position = blockPosition.getValue();
        this.closeReaderOnClose = closeReaderOnClose;
    }

    @Override
    protected void doClose() {
        if (closeReaderOnClose) {
            fileReader.close();
        }
    }

    @Override
    public DataBlock read() {
        final byte[] buffer = new byte[blockSize.getDataBlockSize()];
        final int bytesRead = fileReader.read(buffer);
        if (bytesRead < 0) {
            return null; // End of file reached
        }
        if (bytesRead != blockSize.getDataBlockSize()) {
            throw new IndexException("Unable to read full block");
        }
        DataBlockPosition blockPosition = DataBlockPosition.of(position);
        position += blockSize.getDataBlockSize();
        return new DataBlock(Bytes.of(buffer), blockPosition);
    }

}
