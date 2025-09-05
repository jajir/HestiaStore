package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Commitable;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockFile;
import org.hestiastore.index.datablockfile.DataBlockWriterTx;

public class ChunkStoreWriterTx implements Commitable {

    private final DataBlockWriterTx dataBlockWriterTx;
    private final ChunkStorePosition startPosition;
    private final int payloadSize;

    public ChunkStoreWriterTx(final DataBlockFile blockDataFile,
            final ChunkStorePosition startPosition, final int payloadSize) {
        Vldtn.requireNonNull(blockDataFile, "blockDataFile");
        this.dataBlockWriterTx = blockDataFile.getDataBlockWriterTx();
        this.startPosition = Vldtn.requireNonNull(startPosition,
                "startPosition");
        this.payloadSize = Vldtn.requireCellSize(payloadSize, "payloadSize");
    }

    public ChunkStoreWriter openWriter() {
        return new ChunkStoreWriterImpl(startPosition,
                dataBlockWriterTx.openWriter(), payloadSize);
    }

    @Override
    public void commit() {
        dataBlockWriterTx.commit();
    }

}
