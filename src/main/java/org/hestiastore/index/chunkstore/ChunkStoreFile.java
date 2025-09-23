package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.CellPosition;
import org.hestiastore.index.datablockfile.DataBlockByteReader;
import org.hestiastore.index.datablockfile.DataBlockByteReaderImpl;
import org.hestiastore.index.datablockfile.DataBlockFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;

public class ChunkStoreFile {

    private final DataBlockFile dataBlockFile;
    private final DataBlockSize dataBlockSize;

    public ChunkStoreFile(final Directory directory, final String fileName,
            final DataBlockSize dataBlockSize) {
        this.dataBlockFile = new DataBlockFile(directory, fileName,
                dataBlockSize);
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
    }

    public ChunkStoreReader openReader(final CellPosition chunkPosition) {
        final DataBlockByteReader dataBlockByteReader = new DataBlockByteReaderImpl(
                dataBlockFile
                        .openReader(chunkPosition.getDataBlockStartPosition()),
                dataBlockSize, chunkPosition.getCellIndex());
        return new ChunkStoreReaderImpl(dataBlockByteReader);
    }

    public ChunkStoreWriterTx openWriteTx() {
        return new ChunkStoreWriterTx(dataBlockFile, dataBlockSize);
    }

    public CellPosition getFirstChunkStorePosition() {
        return CellPosition.of(dataBlockSize, 0);
    }

}
