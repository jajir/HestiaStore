package org.hestiastore.index.datablockfile;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.MutableBytes;
import org.hestiastore.index.Vldtn;

/**
 * Implementation of {@link DataBlockByteReader}.
 */
public class DataBlockByteReaderImpl extends AbstractCloseableResource
        implements DataBlockByteReader {

    private final DataBlockReader dataBlockReader;
    private final int dataBlockPayloadSize;

    private DataBlock currentDataBlock = null;
    private int currentBlockPosition = 0;

    /**
     * Creates a new instance of {@link DataBlockByteReaderImpl}.
     *
     * @param dataBlockReader  the data block reader
     * @param dataBlockSize    the data block size
     * @param initialCellIndex the initial cell index to start reading from
     *                         (0-based)
     */
    public DataBlockByteReaderImpl(final DataBlockReader dataBlockReader,
            final DataBlockSize dataBlockSize, final int initialCellIndex) {
        this.dataBlockReader = Vldtn.requireNonNull(dataBlockReader,
                "dataBlockReader");
        Vldtn.requireNonNull(dataBlockSize, "dataBlockSize");
        this.dataBlockPayloadSize = dataBlockSize.getPayloadSize();
        if (initialCellIndex > 0) {
            final int requestedPosition = initialCellIndex * 16;
            Vldtn.requireCellSize(requestedPosition, "requestedPosition");
            if (requestedPosition >= dataBlockPayloadSize) {
                throw new IllegalArgumentException(String.format(
                        "Initial cell index '%s' is out of range. Max allowed is '%s'",
                        initialCellIndex, (dataBlockPayloadSize / 16) - 1));
            }
            moveToNextDataBlock();
            this.currentBlockPosition = requestedPosition;
        }
    }

    @Override
    protected void doClose() {
        dataBlockReader.close();
    }

    @Override
    public ByteSequence readExactly(final int length) {
        Vldtn.requireGreaterThanZero(length, "length");
        int bytesToRead = Vldtn.requireCellSize(length, "length");
        optionalyMoveToNextDataBlock();
        if (currentDataBlock == null) {
            return null;
        }
        final int totalBytesToRead = bytesToRead;
        final MutableBytes result = MutableBytes.allocate(totalBytesToRead);
        int offset = 0;
        while (bytesToRead > 0) {
            final int remainingBytesToReadInChunk = dataBlockPayloadSize
                    - currentBlockPosition;
            final int bytesToReadFromCurrentBlock = Math
                    .min(remainingBytesToReadInChunk, bytesToRead);
            final ByteSequence payload = currentDataBlock.getPayload()
                    .getBytes();
            result.setBytes(offset, payload, currentBlockPosition,
                    bytesToReadFromCurrentBlock);
            offset += bytesToReadFromCurrentBlock;
            currentBlockPosition += bytesToReadFromCurrentBlock;
            bytesToRead -= bytesToReadFromCurrentBlock;
            optionalyMoveToNextDataBlock();
            if (bytesToRead > 0 && currentDataBlock == null) {
                return null;
            }
        }
        return result.toBytes();
    }

    private void optionalyMoveToNextDataBlock() {
        if (currentDataBlock == null
                || currentBlockPosition >= dataBlockPayloadSize) {
            moveToNextDataBlock();
        }
    }

    private void moveToNextDataBlock() {
        currentDataBlock = dataBlockReader.read();
        currentBlockPosition = 0;
    }

}
