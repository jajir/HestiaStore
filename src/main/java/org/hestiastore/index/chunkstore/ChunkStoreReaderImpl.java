package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlock;
import org.hestiastore.index.datablockfile.DataBlockReader;

public class ChunkStoreReaderImpl implements ChunkStoreReader {

    private final DataBlockReader dataBlockReader;
    private final int dataBlockPayloadSize;
    private DataBlock currentDataBlock = null;
    private int currentBlockPosition = 0;

    public ChunkStoreReaderImpl(final DataBlockReader dataBlockReader,
            final int dataBlockPayloadSize) {
        this.dataBlockReader = Vldtn.requireNonNull(dataBlockReader,
                "dataBlockReader");
        this.dataBlockPayloadSize = dataBlockPayloadSize;
    }

    @Override
    public void close() {
        dataBlockReader.close();
    }

    @Override
    public Chunk read() {
        final ChunkHeader chunkHeader = readHeader();
        int requiredLength = chunkHeader.getPayloadLength();
        int cellLength = convertLengthToWholeCells(requiredLength);
        Bytes payload = readPayload(cellLength);
        if (cellLength != requiredLength) {
            payload = payload.subBytes(0, requiredLength);
        }
        return Chunk.of(chunkHeader, payload);
    }

    private ChunkHeader readHeader() {
        optionalyMoveToNextDataBlock();
        final int remainingBytes = dataBlockPayloadSize - currentBlockPosition;
        Bytes tmp = null;
        if (remainingBytes == 16) {
            tmp = currentDataBlock.getPayload().getBytes()
                    .subBytes(currentBlockPosition, currentBlockPosition + 16);
            moveToNextDataBlock();
            tmp = tmp.add(
                    currentDataBlock.getPayload().getBytes().subBytes(0, 16));
            currentBlockPosition = 16;
        } else {
            tmp = currentDataBlock.getPayload().getBytes()
                    .subBytes(currentBlockPosition, 32);
            currentBlockPosition += 32;

        }
        return ChunkHeader.of(tmp);
    }

    private Bytes readPayload(final int length) {
        Vldtn.requireGreaterThanZero(length, "length");
        optionalyMoveToNextDataBlock();
        final int remainingBytes = dataBlockPayloadSize - currentBlockPosition;
        Bytes tmp = null;
        if (remainingBytes == length) {
            tmp = currentDataBlock.getPayload().getBytes()
                    .subBytes(currentBlockPosition, length);
            moveToNextDataBlock();
            tmp = tmp.add(currentDataBlock.getPayload().getBytes().subBytes(0,
                    length));
            currentBlockPosition = length;
        } else {
            tmp = currentDataBlock.getPayload().getBytes().subBytes(
                    currentBlockPosition, currentBlockPosition + length);
            currentBlockPosition += length;

        }
        return tmp;
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

    private int convertLengthToWholeCells(final int length) {
        int out = length / Chunk.CELL_SIZE;
        if (length % Chunk.CELL_SIZE != 0) {
            out++;
        }
        return out * Chunk.CELL_SIZE;

    }

}
