package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockPayload;
import org.hestiastore.index.datablockfile.DataBlockWriter;

public class ChunkStoreWriterImpl implements ChunkStoreWriter {

    private final DataBlockWriter dataBlockWriter;

    private final int dataBlockPayloadSize;

    private ChunkStorePosition chunkStorePosition;

    /**
     * Remaining bytes to write to the next data block.
     */
    private Bytes toNextDataBlock = null;

    public ChunkStoreWriterImpl(final ChunkStorePosition chunkStorePosition,
            final DataBlockWriter dataBlockWriter, final int blockPayloadSize) {
        this.chunkStorePosition = Vldtn.requireNonNull(chunkStorePosition,
                "chunkStorePosition");
        this.dataBlockWriter = Vldtn.requireNonNull(dataBlockWriter,
                "dataBlockWriter");
        this.dataBlockPayloadSize = Vldtn.requireCellSize(blockPayloadSize,
                "blockPayloadSize");
        this.chunkStorePosition = Vldtn.requireNonNull(chunkStorePosition,
                "chunkStorePosition");
    }

    @Override
    public void close() {
        if (toNextDataBlock != null && toNextDataBlock.length() > 0) {
            toNextDataBlock = toNextDataBlock.paddedTo(dataBlockPayloadSize);
            dataBlockWriter.write(DataBlockPayload.of(toNextDataBlock));
        }
        dataBlockWriter.close();
    }

    @Override
    public ChunkStorePosition write(final ChunkPayload chunkPayload,
            int version) {
        Vldtn.requireNonNull(chunkPayload, "chunkPayload");
        final ChunkStorePosition returnPosition = chunkStorePosition;
        final ChunkHeader header = ChunkHeader.of(Chunk.MAGIC_NUMBER, version,
                chunkPayload.length(), chunkPayload.calculateCrc());
        Bytes bufferToWrite = Bytes
                .of(header.getBytes(), chunkPayload.getBytes())
                .paddedToNextCell();

        while (bufferToWrite != null && bufferToWrite.length() > 0) {
            final int occupiedBytes = chunkStorePosition.getOccupiedBytes();
            final int availableBytes = dataBlockPayloadSize - occupiedBytes;
            if (availableBytes <= 0) {
                throw new IllegalStateException("Data block is full");
            }
            if (availableBytes < bufferToWrite.length()) {
                Bytes tmp = null;
                if (toNextDataBlock != null && toNextDataBlock.length() > 0) {
                    tmp = Bytes.of(toNextDataBlock,
                            bufferToWrite.subBytes(0, availableBytes));
                } else {
                    tmp = bufferToWrite.subBytes(0, availableBytes);
                }
                bufferToWrite = bufferToWrite.subBytes(availableBytes,
                        bufferToWrite.length());
                chunkStorePosition = chunkStorePosition
                        .addCellsForBytes(availableBytes);
                dataBlockWriter.write(DataBlockPayload.of(tmp));
                toNextDataBlock = null;
            } else {
                chunkStorePosition = chunkStorePosition
                        .addCellsForBytes(bufferToWrite.length());
                if (toNextDataBlock == null || toNextDataBlock.length() == 0) {
                    toNextDataBlock = bufferToWrite;
                } else {
                    toNextDataBlock = Bytes.of(toNextDataBlock, bufferToWrite);
                }
                bufferToWrite = null;
            }

        }
        return returnPosition;
    }

}
