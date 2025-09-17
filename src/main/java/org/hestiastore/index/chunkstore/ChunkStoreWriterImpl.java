package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

public class ChunkStoreWriterImpl implements ChunkStoreWriter {

    private final CellStoreWriter cellStoreWriter;

    public ChunkStoreWriterImpl(final CellStoreWriter cellStoreWriter) {
        this.cellStoreWriter = Vldtn.requireNonNull(cellStoreWriter,
                "cellStoreWriter");
    }

    @Override
    public void close() {
        cellStoreWriter.close();
    }

    @Override
    public CellPosition write(final ChunkPayload chunkPayload,
            final int version) {
        Vldtn.requireNonNull(chunkPayload, "chunkPayload");
        final ChunkHeader header = ChunkHeader.of(Chunk.MAGIC_NUMBER, version,
                chunkPayload.length(), chunkPayload.calculateCrc());
        final Bytes bufferToWrite = Bytes
                .of(header.getBytes(), chunkPayload.getBytes())
                .paddedToNextCell();
        return cellStoreWriter.write(bufferToWrite);
    }

}
