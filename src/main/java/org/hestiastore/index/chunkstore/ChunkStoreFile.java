package org.hestiastore.index.chunkstore;

import org.hestiastore.index.datablockfile.DataBlockFile;
import org.hestiastore.index.directory.Directory;

public class ChunkStoreFile {

    private final DataBlockFile dataBlockFile;
    private final int dataBlockSize;

    public ChunkStoreFile(final Directory directory, final String fileName,
            final int blockSize) {
        this.dataBlockFile = new DataBlockFile(directory, fileName, blockSize);
        this.dataBlockSize = blockSize;
    }

    public ChunkStoreReader openReader(final ChunkStorePosition chunkPosition) {
        final DataBlockByteReader dataBlockByteReader = new DataBlockByteReaderImpl(
                dataBlockFile.openReader(chunkPosition.getDataBlockPosition()),
                dataBlockSize, chunkPosition.getCellIndex());
        return new ChunkStoreReaderImpl(dataBlockByteReader);
    }

    public ChunkStoreWriterTx openWriteTx() {
        return new ChunkStoreWriterTx(dataBlockFile,
                getFirstChunkStorePosition(),
                dataBlockFile.getDataBlockPayloadSize());
    }

    public ChunkStorePosition getFirstChunkStorePosition() {
        return ChunkStorePosition.of(dataBlockSize, 0);
    }

}
