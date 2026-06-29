package org.hestiastore.index.chunkstore;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.datablockfile.DataBlockByteReader;
import org.hestiastore.index.datablockfile.DataBlockByteReaderImpl;
import org.hestiastore.index.datablockfile.DataBlockFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datablockfile.Reader;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReaderSeekable;

/**
 * Accessor and factory for chunk data stored in a single chunk-store file.
 *
 * <p>
 * This type keeps long-lived file configuration together with chunk filter
 * chain factories. Concrete filter instances are materialized only when a
 * reader, payload reader, or writer transaction is opened.
 * </p>
 */
public class ChunkStoreFile {

    private final DataBlockFile dataBlockFile;
    private final DataBlockSize dataBlockSize;
    private final ChunkFilterChainFactory encodingChunkFilters;
    private final ChunkFilterChainFactory decodingChunkFilters;

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
        this(directoryFacade, fileName, dataBlockSize,
                ChunkFilterChainFactory.fromFilters(encodingChunkFilters),
                ChunkFilterChainFactory.fromFilters(decodingChunkFilters));
    }

    /**
     * Creates a chunk-store accessor backed by runtime filter suppliers.
     *
     * <p>
     * Use this variant when a fresh filter instance may be needed for each
     * reader or writer, for example for stateful or non-thread-safe filters.
     * </p>
     *
     * @param directoryFacade required directory containing the file
     * @param fileName required chunk-store file name
     * @param dataBlockSize required data block size
     * @param encodingChunkFilters required write-path filter suppliers
     * @param decodingChunkFilters required read-path filter suppliers
     * @return new chunk-store accessor
     */
    public static ChunkStoreFile fromSuppliers(final Directory directoryFacade,
            final String fileName,
            final DataBlockSize dataBlockSize,
            final List<? extends Supplier<? extends ChunkFilter>> encodingChunkFilters,
            final List<? extends Supplier<? extends ChunkFilter>> decodingChunkFilters) {
        return new ChunkStoreFile(directoryFacade, fileName, dataBlockSize,
                ChunkFilterChainFactory.fromSuppliers(encodingChunkFilters),
                ChunkFilterChainFactory.fromSuppliers(decodingChunkFilters));
    }

    private ChunkStoreFile(final Directory directoryFacade,
            final String fileName,
            final DataBlockSize dataBlockSize,
            final ChunkFilterChainFactory encodingChunkFilters,
            final ChunkFilterChainFactory decodingChunkFilters) {
        this.dataBlockFile = new DataBlockFile(directoryFacade, fileName,
                dataBlockSize);
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
        this.encodingChunkFilters = Vldtn.requireNonNull(encodingChunkFilters,
                "encodingChunkFilters");
        this.decodingChunkFilters = Vldtn.requireNonNull(decodingChunkFilters,
                "decodingChunkFilters");
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

    /**
     * Opens a chunk reader, optionally reusing an externally owned seekable
     * reader.
     *
     * <p>
     * The decoding filter chain is materialized for the returned runtime
     * reader.
     * </p>
     *
     * @param chunkPosition required position of the chunk to read
     * @param seekableReader optional externally managed seekable reader;
     *                       {@code null} creates a dedicated reader
     * @return reader positioned at the requested chunk
     */
    public ChunkStoreReader openReader(final CellPosition chunkPosition,
            final FileReaderSeekable seekableReader) {
        final DataBlockByteReader dataBlockByteReader = new DataBlockByteReaderImpl(
                dataBlockFile
                        .openReader(chunkPosition.getDataBlockStartPosition(),
                                seekableReader),
                dataBlockSize, chunkPosition.getCellIndex());
        return new ChunkStoreReaderImpl(dataBlockByteReader,
                decodingChunkFilters.materialize());
    }

    /**
     * Opens a non-closeable payload reader for point lookups using an externally
     * owned seekable reader.
     *
     * <p>
     * The caller owns {@code seekableReader} lifecycle. Returned reader is
     * intentionally lightweight and does not cascade close operations.
     * </p>
     *
     * @param chunkPosition required starting chunk position
     * @param seekableReader required externally-managed seekable reader
     * @return reader of decoded chunk payload sequences
     */
    public Reader<ByteSequence> openPayloadReader(final CellPosition chunkPosition,
            final FileReaderSeekable seekableReader) {
        final CellPosition resolvedChunkPosition = Vldtn
                .requireNonNull(chunkPosition, "chunkPosition");
        final FileReaderSeekable resolvedSeekableReader = Vldtn
                .requireNonNull(seekableReader, "seekableReader");
        final DataBlockByteReader dataBlockByteReader = new DataBlockByteReaderImpl(
                dataBlockFile.openReader(
                        resolvedChunkPosition.getDataBlockStartPosition(),
                        resolvedSeekableReader),
                dataBlockSize, resolvedChunkPosition.getCellIndex());
        final ChunkProcessor decodingProcessor = new ChunkProcessor(
                decodingChunkFilters.materialize());
        return () -> {
            final Optional<ChunkData> optionalChunkData = ChunkData
                    .read(dataBlockByteReader);
            if (optionalChunkData.isEmpty()) {
                return null;
            }
            return decodingProcessor.process(optionalChunkData.get())
                    .getPayloadSequence();
        };
    }

    /**
     * Opens a writer transaction for writing chunks to the chunk store file.
     *
     * @return a ChunkStoreWriterTx for writing chunks
     */
    public ChunkStoreWriterTx openWriteTx() {
        return new ChunkStoreWriterTx(dataBlockFile, dataBlockSize,
                encodingChunkFilters.materialize());
    }

    /**
     * Gets the position of the first chunk in the chunk store file.
     *
     * @return the CellPosition of the first chunk
     */
    public CellPosition getFirstChunkStorePosition() {
        return CellPosition.of(dataBlockSize, 0);
    }

    /**
     * Materializes the encoding filter chain currently configured for this
     * file.
     *
     * @return encoding filters for one runtime use
     */
    List<ChunkFilter> getEncodingChunkFilters() {
        return encodingChunkFilters.materialize();
    }

    /**
     * Materializes the decoding filter chain currently configured for this
     * file.
     *
     * @return decoding filters for one runtime use
     */
    List<ChunkFilter> getDecodingChunkFilters() {
        return decodingChunkFilters.materialize();
    }

    /**
     * Returns immutable encoding suppliers used to create runtime write-path
     * filters.
     *
     * @return encoding filter suppliers
     */
    List<Supplier<? extends ChunkFilter>> getEncodingChunkFilterSuppliers() {
        return encodingChunkFilters.getSuppliers();
    }

    /**
     * Returns immutable decoding suppliers used to create runtime read-path
     * filters.
     *
     * @return decoding filter suppliers
     */
    List<Supplier<? extends ChunkFilter>> getDecodingChunkFilterSuppliers() {
        return decodingChunkFilters.getSuppliers();
    }

}
