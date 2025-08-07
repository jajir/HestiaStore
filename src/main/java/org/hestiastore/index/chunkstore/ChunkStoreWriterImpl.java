package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.blockdatafile.DataBlockPayload;
import org.hestiastore.index.blockdatafile.DataBlockWriter;

public class ChunkStoreWriterImpl implements ChunkStoreWriter {

    private final DataBlockWriter dataBlockWriter;

    private final int dataBlockSize;

    private final int dataBlockPayloadSize;

    private ChunkStorePosition chunkStorePosition;

    /**
     * Remaining bytes to write to the next data block.
     */
    private Bytes toNextDataBlock = null;

    public ChunkStoreWriterImpl(final DataBlockWriter dataBlockWriter,
            final int dataBlockSize, final int blockPayloadSize) {
        this.dataBlockWriter = Vldtn.requireNonNull(dataBlockWriter,
                "dataBlockWriter");
        this.dataBlockPayloadSize = blockPayloadSize;
        this.dataBlockSize = dataBlockSize;
        chunkStorePosition = ChunkStoreFile.FIRST_POSITION;
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
        Bytes bytesToWrite = Bytes.of(header.toBytes(),
                chunkPayload.getBytes());
        if (chunkStorePosition.cellIndex() > 0) {
            // find remaining bytes and write to them part of chunk payload
            final int occupiedBytes = chunkStorePosition.cellIndex()
                    * Chunk.CELL_SIZE;
            final int freeBytes = dataBlockPayloadSize - occupiedBytes;
            if (freeBytes > 0) {
                // TODO
                toNextDataBlock = bytesToWrite.subBytes(0, freeBytes);
                bytesToWrite = bytesToWrite.subBytes(freeBytes,
                        bytesToWrite.length());
                dataBlockWriter.write(DataBlockPayload.of(toNextDataBlock));
            }
        }
        while (bytesToWrite.length() > dataBlockPayloadSize) {
            final Bytes tmp = bytesToWrite.subBytes(0, dataBlockPayloadSize);
            bytesToWrite = bytesToWrite.subBytes(dataBlockPayloadSize,
                    bytesToWrite.length());
            dataBlockWriter.write(DataBlockPayload.of(tmp));
            chunkStorePosition = chunkStorePosition.addDataBlock(dataBlockSize);
        }
        if (bytesToWrite.length() > 0) {
            chunkStorePosition = chunkStorePosition
                    .addCellsForBytes(bytesToWrite.length());
            bytesToWrite = bytesToWrite.paddedTo(dataBlockPayloadSize);
            toNextDataBlock = bytesToWrite;
        } else {
            toNextDataBlock = null;
        }
        return returnPosition;
    }

}
