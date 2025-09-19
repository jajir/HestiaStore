package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Commitable;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datablockfile.DataBlockWriterTx;

public class ChunkStoreWriterTx implements Commitable {

    private final DataBlockWriterTx dataBlockWriterTx;
    private final DataBlockSize dataBlockSize;

    public ChunkStoreWriterTx(final DataBlockFile blockDataFile,
            final DataBlockSize dataBlockSize) {
        Vldtn.requireNonNull(blockDataFile, "blockDataFile");
        this.dataBlockWriterTx = blockDataFile.getDataBlockWriterTx();
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
    }

    public ChunkStoreWriter openWriter() {
        final CellStoreWriterCursor cursor = new CellStoreWriterCursor(
                dataBlockWriterTx.openWriter(), dataBlockSize);
        final CellStoreWriterImpl cellStoreWriter = new CellStoreWriterImpl(
                cursor);
        return new ChunkStoreWriterImpl(cellStoreWriter);
    }

    @Override
    public void commit() {
        dataBlockWriterTx.commit();
    }

}
