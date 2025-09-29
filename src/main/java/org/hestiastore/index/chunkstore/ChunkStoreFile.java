package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockByteReader;
import org.hestiastore.index.datablockfile.DataBlockByteReaderImpl;
import org.hestiastore.index.datablockfile.DataBlockFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;

/**
 * A file that stores chunks of data in a chunk store.
 */
public class ChunkStoreFile {

    private final DataBlockFile dataBlockFile;
    private final DataBlockSize dataBlockSize;

    /**
     * Constructs a new ChunkStoreFile.
     *
     * @param directory     required directory where the chunk store file is
     *                      located
     * @param fileName      required name of the chunk store file
     * @param dataBlockSize required size of the data blocks in the chunk store
     *                      file
     */
    public ChunkStoreFile(final Directory directory, final String fileName,
            final DataBlockSize dataBlockSize) {
        this.dataBlockFile = new DataBlockFile(directory, fileName,
                dataBlockSize);
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
    }

    /**
     * Opens a reader for the specified chunk position.
     *
     * @param chunkPosition required position of the chunk to read
     * @return a ChunkStoreReader for reading the chunk
     */
    public ChunkStoreReader openReader(final CellPosition chunkPosition) {
        final DataBlockByteReader dataBlockByteReader = new DataBlockByteReaderImpl(
                dataBlockFile
                        .openReader(chunkPosition.getDataBlockStartPosition()),
                dataBlockSize, chunkPosition.getCellIndex());
        return new ChunkStoreReaderImpl(dataBlockByteReader);
    }

    /**
     * Opens a writer transaction for writing chunks to the chunk store file.
     *
     * @return a ChunkStoreWriterTx for writing chunks
     */
    public ChunkStoreWriterTx openWriteTx() {
        return new ChunkStoreWriterTx(dataBlockFile, dataBlockSize);
    }

    /**
     * Gets the position of the first chunk in the chunk store file.
     *
     * @return the CellPosition of the first chunk
     */
    public CellPosition getFirstChunkStorePosition() {
        return CellPosition.of(dataBlockSize, 0);
    }

}
