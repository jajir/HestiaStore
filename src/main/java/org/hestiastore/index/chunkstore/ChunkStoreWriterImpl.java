package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

/**
 * A writer for writing chunks to a chunk store.
 */
public class ChunkStoreWriterImpl implements ChunkStoreWriter {

    private final CellStoreWriter cellStoreWriter;

    /**
     * Creates a new instance of {@link ChunkStoreWriterImpl}.
     *
     * @param cellStoreWriter required cell store writer to write chunk data to.
     */
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
        final ChunkHeader header = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                version, chunkPayload.length(), chunkPayload.calculateCrc());
        final Bytes bufferToWrite = Bytes
                .concat(header.getBytes(), chunkPayload.getBytes())
                .paddedToNextCell();
        return cellStoreWriter.write(bufferToWrite);
    }

}
