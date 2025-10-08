package org.hestiastore.index.chunkstore;

import java.util.List;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

/**
 * A writer for writing chunks to a chunk store.
 */
public class ChunkStoreWriterImpl implements ChunkStoreWriter {

    private static final long DEFAULT_LONG = -1L;

    private final CellStoreWriter cellStoreWriter;
    private final ChunkProcessor encodingProcessor;

    /**
     * Creates a new instance of {@link ChunkStoreWriterImpl}.
     *
     * @param cellStoreWriter required cell store writer to write chunk data to.
     */
    public ChunkStoreWriterImpl(final CellStoreWriter cellStoreWriter,
            final List<ChunkFilter> encodingChunkFilters) {
        this.cellStoreWriter = Vldtn.requireNonNull(cellStoreWriter,
                "cellStoreWriter");
        final List<ChunkFilter> filters = List
                .copyOf(Vldtn.requireNonNull(encodingChunkFilters, "filters"));
        this.encodingProcessor = new ChunkProcessor(filters);
    }

    @Override
    public void close() {
        cellStoreWriter.close();
    }

    @Override
    public CellPosition write(final ChunkPayload chunkPayload,
            final int version) {
        Vldtn.requireNonNull(chunkPayload, "chunkPayload");
        ChunkData chunkData = ChunkData.of(DEFAULT_LONG, DEFAULT_LONG,
                DEFAULT_LONG, version, chunkPayload.getBytes());
        chunkData = encodingProcessor.process(chunkData);
        final ChunkHeader header = ChunkHeader.of(chunkData.getMagicNumber(),
                chunkData.getVersion(), chunkData.getPayload().length(),
                chunkData.getCrc(), chunkData.getFlags());
        final Bytes bufferToWrite = Bytes
                .concat(header.getBytes(), chunkData.getPayload())
                .paddedToNextCell();
        return cellStoreWriter.write(bufferToWrite);
    }

}
