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
    public Chunk read() {
        final ChunkHeader chunkHeader = readHeader();
        if (chunkHeader == null) {
            return null;
        }
        int requiredLength = chunkHeader.getPayloadLength();
        int cellLength = convertLengthToWholeCells(requiredLength);
        Bytes payload = readPayload(cellLength);
        if (cellLength != requiredLength) {
            payload = payload.subBytes(0, requiredLength);
        }
        final Chunk out = Chunk.of(chunkHeader, payload);
        if (chunkHeader.getCrc() != out.calculateCrc()) {
            throw new IllegalArgumentException(String.format(
                    "Invalid chunk CRC, expected: '%s', actual: '%s'",
                    chunkHeader.getCrc(), out.calculateCrc()));
        }
        return out;
    }

    private ChunkHeader readHeader() {
        optionalyMoveToNextDataBlock();
        if (currentDataBlock == null) {
            return null;
        }
        final int remainingBytes = dataBlockPayloadSize - currentBlockPosition;
        Bytes tmp = null;
        if (remainingBytes == 16) {
            tmp = currentDataBlock.getPayload().getBytes()
                    .subBytes(currentBlockPosition, currentBlockPosition + 16);
            moveToNextDataBlock();
            if (currentDataBlock == null) {
                return null;
            }
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
        int bytesToread = Vldtn.requireGreaterThanZero(length, "length");
        optionalyMoveToNextDataBlock();
        Bytes chunkPayloadBytes = Bytes.EMPTY;
        while (bytesToread > 0) {
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
            if (dataBlockPayloadSize <= currentBlockPosition) {
                moveToNextDataBlock();
            }
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

    private int convertLengthToWholeCells(final int length) {
        int out = length / Chunk.CELL_SIZE;
        if (length % Chunk.CELL_SIZE != 0) {
            out++;
        }
        return out * Chunk.CELL_SIZE;

    }

}
