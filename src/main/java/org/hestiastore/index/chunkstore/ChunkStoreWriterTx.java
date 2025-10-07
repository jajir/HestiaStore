package org.hestiastore.index.chunkstore;

import java.util.List;

import org.hestiastore.index.Commitable;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datablockfile.DataBlockWriterTx;

/**
 * A transaction for writing chunks to a chunk store.
 */
public class ChunkStoreWriterTx implements Commitable {

    private final DataBlockWriterTx dataBlockWriterTx;
    private final DataBlockSize dataBlockSize;
    private final List<ChunkFilter> encodingChunkFilters;

    /**
     * Creates a new chunk store writer transaction.
     * 
     * @param blockDataFile required block data file to write chunks to.
     * @param dataBlockSize required data block size for the chunk store.
     */
    public ChunkStoreWriterTx(final DataBlockFile blockDataFile,
            final DataBlockSize dataBlockSize,
            final List<ChunkFilter> encodingChunkFilters) {
        Vldtn.requireNonNull(blockDataFile, "blockDataFile");
        this.dataBlockWriterTx = blockDataFile.getDataBlockWriterTx();
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
        this.encodingChunkFilters = List
                .copyOf(Vldtn.requireNonNull(encodingChunkFilters, "filters"));
    }

    /**
     * Opens a new chunk store writer.
     * 
     * @return the opened chunk store writer.
     */
    public ChunkStoreWriter openWriter() {
        final CellStoreWriterCursor cursor = new CellStoreWriterCursor(
                dataBlockWriterTx.openWriter(), dataBlockSize);
        final CellStoreWriterImpl cellStoreWriter = new CellStoreWriterImpl(
                cursor);
        return new ChunkStoreWriterImpl(cellStoreWriter, encodingChunkFilters);
    }

    @Override
    public void commit() {
        dataBlockWriterTx.commit();
    }

}
