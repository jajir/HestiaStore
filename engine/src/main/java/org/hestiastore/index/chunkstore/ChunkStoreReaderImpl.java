package org.hestiastore.index.chunkstore;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockByteReader;

/**
 * Implementation of {@link ChunkStoreReader}.
 */
public class ChunkStoreReaderImpl extends AbstractCloseableResource
        implements ChunkStoreReader {

    private DataBlockByteReader dataBlockByteReader = null;
    private final ChunkProcessor decodingProcessor;

    /**
     * Constructor.
     *
     * @param dataBlockByteReader required data block byte reader
     * @throws IllegalArgumentException when dataBlockByteReader is null
     */
    public ChunkStoreReaderImpl(final DataBlockByteReader dataBlockByteReader,
            final List<ChunkFilter> decodingChunkFilters) {
        this.dataBlockByteReader = Vldtn.requireNonNull(dataBlockByteReader,
                "dataBlockByteReader");
        final List<ChunkFilter> filters = List.copyOf(Vldtn
                .requireNonNull(decodingChunkFilters, "decodingChunkFilters"));
        this.decodingProcessor = new ChunkProcessor(filters);
    }

    @Override
    protected void doClose() {
        dataBlockByteReader.close();
    }

    @Override
    public Chunk read() {
        final Optional<ChunkData> optionalChunkData = ChunkData
                .read(dataBlockByteReader);
        if (optionalChunkData.isEmpty()) {
            return null;
        }
        final ChunkData chunkData = decodingProcessor
                .process(optionalChunkData.get());
        final ChunkHeader chunkHeader = ChunkHeader.of(
                chunkData.getMagicNumber(), chunkData.getVersion(),
                chunkData.getPayload().length(), chunkData.getCrc(),
                chunkData.getFlags());
        return Chunk.of(chunkHeader, chunkData.getPayload());
    }
}
