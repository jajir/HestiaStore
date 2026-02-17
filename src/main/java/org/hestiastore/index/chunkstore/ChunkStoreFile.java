package org.hestiastore.index.chunkstore;

import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockByteReader;
import org.hestiastore.index.datablockfile.DataBlockByteReaderImpl;
import org.hestiastore.index.datablockfile.DataBlockFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReaderSeekable;

/**
 * A file that stores chunks of data in a chunk store.
 */
public class ChunkStoreFile {

    private final DataBlockFile dataBlockFile;
    private final DataBlockSize dataBlockSize;
    private final List<ChunkFilter> encodingChunkFilters;
    private final List<ChunkFilter> decodingChunkFilters;

    /**
     * Constructs a new ChunkStoreFile.
     *
     * @param directoryFacade required directory where the chunk store file is
     *                        located
     * @param fileName      required name of the chunk store file
     * @param dataBlockSize required size of the data blocks in the chunk store
     *                      file
     */
    public ChunkStoreFile(final Directory directoryFacade,
            final String fileName,
            final DataBlockSize dataBlockSize,
            final List<ChunkFilter> encodingChunkFilters,
            final List<ChunkFilter> decodingChunkFilters) {
        this.dataBlockFile = new DataBlockFile(directoryFacade, fileName,
                dataBlockSize);
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
        this.encodingChunkFilters = List.copyOf(encodingChunkFilters);
        this.decodingChunkFilters = List.copyOf(decodingChunkFilters);
    }

    /**
     * Opens a reader for the specified chunk position.
     *
     * @param chunkPosition required position of the chunk to read
     * @return a ChunkStoreReader for reading the chunk
     */
    public ChunkStoreReader openReader(final CellPosition chunkPosition) {
        return openReader(chunkPosition, null);
    }

    public ChunkStoreReader openReader(final CellPosition chunkPosition,
            final FileReaderSeekable seekableReader) {
        final DataBlockByteReader dataBlockByteReader = new DataBlockByteReaderImpl(
                dataBlockFile
                        .openReader(chunkPosition.getDataBlockStartPosition(),
                                seekableReader),
                dataBlockSize, chunkPosition.getCellIndex());
        return new ChunkStoreReaderImpl(dataBlockByteReader,
                decodingChunkFilters);
    }

    /**
     * Opens a writer transaction for writing chunks to the chunk store file.
     *
     * @return a ChunkStoreWriterTx for writing chunks
     */
    public ChunkStoreWriterTx openWriteTx() {
        return new ChunkStoreWriterTx(dataBlockFile, dataBlockSize,
                encodingChunkFilters);
    }

    /**
     * Gets the position of the first chunk in the chunk store file.
     *
     * @return the CellPosition of the first chunk
     */
    public CellPosition getFirstChunkStorePosition() {
        return CellPosition.of(dataBlockSize, 0);
    }

    List<ChunkFilter> getEncodingChunkFilters() {
        return encodingChunkFilters;
    }

    List<ChunkFilter> getDecodingChunkFilters() {
        return decodingChunkFilters;
    }

}
