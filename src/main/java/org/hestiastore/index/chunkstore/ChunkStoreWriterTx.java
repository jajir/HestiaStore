package org.hestiastore.index.chunkstore;

import java.util.List;

import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datablockfile.DataBlockWriter;
import org.hestiastore.index.datablockfile.DataBlockWriterTx;

public class ChunkStoreWriterTx
        extends GuardedWriteTransaction<ChunkStoreWriter> {

    private final DataBlockWriterTx dataBlockWriterTx;
    private final DataBlockSize dataBlockSize;
    private final List<ChunkFilter> encodingChunkFilters;

    public ChunkStoreWriterTx(final DataBlockFile blockDataFile,
            final DataBlockSize dataBlockSize,
            final List<ChunkFilter> encodingChunkFilters) {
        Vldtn.requireNonNull(blockDataFile, "blockDataFile");
        this.dataBlockWriterTx = blockDataFile.openWriterTx();
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
        this.encodingChunkFilters = List
                .copyOf(Vldtn.requireNonNull(encodingChunkFilters, "filters"));
    }

    @Override
    protected ChunkStoreWriter doOpen() {
        final DataBlockWriter dataBlockWriter = dataBlockWriterTx.open();
        final CellStoreWriterCursor cursor = new CellStoreWriterCursor(
                dataBlockWriter, dataBlockSize);
        final CellStoreWriterImpl cellStoreWriter = new CellStoreWriterImpl(
                cursor);
        return new ChunkStoreWriterImpl(cellStoreWriter, encodingChunkFilters);
    }

    @Override
    protected void doCommit(final ChunkStoreWriter resource) {
        dataBlockWriterTx.commit();
    }
}
