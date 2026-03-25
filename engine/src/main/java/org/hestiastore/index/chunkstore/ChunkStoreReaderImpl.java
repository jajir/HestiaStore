package org.hestiastore.index.chunkstore;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.datablockfile.DataBlockByteReader;

/**
 * Runtime reader over a single chunk-store stream.
 *
 * <p>
 * The decoding filter chain is already materialized when this object is
 * created. The reader therefore contains only runtime state and does not own
 * any supplier or provider-based configuration.
 * </p>
 */
public class ChunkStoreReaderImpl extends AbstractCloseableResource
        implements ChunkStoreReader {

    private DataBlockByteReader dataBlockByteReader = null;
    private final ChunkProcessor decodingProcessor;

    /**
     * Constructor.
     *
     * @param dataBlockByteReader required data block byte reader
     * @param decodingChunkFilters required decoding filters already resolved
     *                             for this runtime reader
     * @throws IllegalArgumentException when dataBlockByteReader is null
     */
    public ChunkStoreReaderImpl(final DataBlockByteReader dataBlockByteReader,
            final List<ChunkFilter> decodingChunkFilters) {
        this.dataBlockByteReader = Vldtn.requireNonNull(dataBlockByteReader,
                "dataBlockByteReader");
        this.decodingProcessor = new ChunkProcessor(List
                .copyOf(Vldtn.requireNonNull(decodingChunkFilters,
                        "decodingChunkFilters")));
    }

    @Override
    protected void doClose() {
        dataBlockByteReader.close();
    }

    @Override
    public Chunk read() {
        final ChunkData chunkData = readChunkData();
        if (chunkData == null) {
            return null;
        }
        final ByteSequence payload = chunkData.getPayloadSequence();
        final ChunkHeader chunkHeader = ChunkHeader.of(
                chunkData.getMagicNumber(), chunkData.getVersion(),
                payload.length(), chunkData.getCrc(),
                chunkData.getFlags());
        return Chunk.of(chunkHeader, payload);
    }

    @Override
    public ByteSequence readPayloadSequence() {
        final ChunkData chunkData = readChunkData();
        if (chunkData == null) {
            return null;
        }
        return chunkData.getPayloadSequence();
    }

    private ChunkData readChunkData() {
        final Optional<ChunkData> optionalChunkData = ChunkData
                .read(dataBlockByteReader);
        if (optionalChunkData.isEmpty()) {
            return null;
        }
        return decodingProcessor.process(optionalChunkData.get());
    }
}
