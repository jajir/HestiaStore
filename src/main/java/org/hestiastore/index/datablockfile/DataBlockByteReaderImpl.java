package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

public class DataBlockByteReaderImpl implements DataBlockByteReader {

    private final DataBlockReader dataBlockReader;
    private final int dataBlockPayloadSize;

    private DataBlock currentDataBlock = null;
    private int currentBlockPosition = 0;

    public DataBlockByteReaderImpl(final DataBlockReader dataBlockReader,
            final int dataBlockPayloadSize, final int initialCellIndex) {
        this.dataBlockReader = Vldtn.requireNonNull(dataBlockReader,
                "dataBlockReader");
        this.dataBlockPayloadSize = dataBlockPayloadSize;
        this.currentBlockPosition = initialCellIndex * 16;
    }

    @Override
    public void close() {
        dataBlockReader.close();

    }

    @Override
    public Bytes readExactly(final int length) {
        Vldtn.requireGreaterThanZero(length, "length");
        int bytesToread = Vldtn.requireCellSize(length, "length");
        optionalyMoveToNextDataBlock();
        Bytes chunkPayloadBytes = Bytes.EMPTY;
        while (bytesToread > 0) {
            if (currentDataBlock == null) {
                return null;
            }
            final int remainingBytesToReadInChunk = dataBlockPayloadSize
                    - currentBlockPosition;
            final int bytesToreadFromCurrentBlock = Math
                    .min(remainingBytesToReadInChunk, bytesToread);
            chunkPayloadBytes = chunkPayloadBytes
                    .add(currentDataBlock.getPayload().getBytes()
                            .subBytes(currentBlockPosition, currentBlockPosition
                                    + bytesToreadFromCurrentBlock));
            currentBlockPosition += bytesToreadFromCurrentBlock;
            bytesToread -= bytesToreadFromCurrentBlock;
            optionalyMoveToNextDataBlock();
        }
        return chunkPayloadBytes;
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
