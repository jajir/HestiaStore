package org.hestiastore.index.chunkstore;

import java.util.List;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.bytes.ConcatenatedByteSequence;

/**
 * Runtime writer for appending chunks to a chunk store.
 *
 * <p>
 * The encoding filter chain is already materialized when this object is
 * created. Any supplier-based lifecycle is resolved by higher-level factory
 * types before the writer is opened.
 * </p>
 */
public class ChunkStoreWriterImpl extends AbstractCloseableResource
        implements ChunkStoreWriter {

    private static final long DEFAULT_FLAGS = 0L;
    private static final long DEFAULT_CRC = 0L;

    private final CellStoreWriter cellStoreWriter;
    private final ChunkProcessor encodingProcessor;

    /**
     * Creates a new instance of {@link ChunkStoreWriterImpl}.
     *
     * @param cellStoreWriter required cell store writer to write chunk data to.
     * @param encodingChunkFilters required encoding filters already resolved
     *                             for this runtime writer
     */
    public ChunkStoreWriterImpl(final CellStoreWriter cellStoreWriter,
            final List<ChunkFilter> encodingChunkFilters) {
        this.cellStoreWriter = Vldtn.requireNonNull(cellStoreWriter,
                "cellStoreWriter");
        this.encodingProcessor = new ChunkProcessor(List
                .copyOf(Vldtn.requireNonNull(encodingChunkFilters,
                        "encodingChunkFilters")));
    }

    @Override
    protected void doClose() {
        cellStoreWriter.close();
    }

    @Override
    public CellPosition writeSequence(final ByteSequence chunkPayload,
            final int version) {
        final ByteSequence payload = Vldtn.requireNonNull(chunkPayload,
                "chunkPayload");
        ChunkData chunkData = ChunkData.ofSequence(DEFAULT_FLAGS, DEFAULT_CRC,
                ChunkHeader.MAGIC_NUMBER, version, payload);
        chunkData = encodingProcessor.process(chunkData);
        if (chunkData.getMagicNumber() != ChunkHeader.MAGIC_NUMBER) {
            chunkData = chunkData.withMagicNumber(ChunkHeader.MAGIC_NUMBER);
        }
        final ByteSequence encodedPayload = chunkData.getPayloadSequence();
        final ChunkHeader header = ChunkHeader.of(chunkData.getMagicNumber(),
                chunkData.getVersion(), encodedPayload.length(),
                chunkData.getCrc(), chunkData.getFlags());
        final ByteSequence headerSequence = header.getBytesSequence();
        final ByteSequence paddedPayload = ByteSequences
                .padToCell(encodedPayload, CellPosition.CELL_SIZE);
        final ByteSequence chunkBytes = ConcatenatedByteSequence
                .of(headerSequence, paddedPayload);
        return cellStoreWriter.writeSequence(chunkBytes);
    }

}
