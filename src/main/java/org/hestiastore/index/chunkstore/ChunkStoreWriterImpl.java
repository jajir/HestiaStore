package org.hestiastore.index.chunkstore;

import java.util.List;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.Bytes;
import org.hestiastore.index.bytes.ConcatenatedByteSequence;

/**
 * A writer for writing chunks to a chunk store.
 */
public class ChunkStoreWriterImpl extends AbstractCloseableResource
        implements ChunkStoreWriter {

    private static final long DEFAULT_LONG = -1L;

    private final CellStoreWriter cellStoreWriter;
    private final ChunkProcessor encodingProcessor;

    /**
     * Creates a new instance of {@link ChunkStoreWriterImpl}.
     *
     * @param cellStoreWriter required cell store writer to write chunk data to.
     */
    public ChunkStoreWriterImpl(final CellStoreWriter cellStoreWriter,
            final List<ChunkFilter> encodingChunkFilters) {
        this.cellStoreWriter = Vldtn.requireNonNull(cellStoreWriter,
                "cellStoreWriter");
        final List<ChunkFilter> filters = List
                .copyOf(Vldtn.requireNonNull(encodingChunkFilters, "filters"));
        this.encodingProcessor = new ChunkProcessor(filters);
    }

    @Override
    protected void doClose() {
        cellStoreWriter.close();
    }

    @Override
    public CellPosition write(final ChunkPayload chunkPayload,
            final int version) {
        Vldtn.requireNonNull(chunkPayload, "chunkPayload");
        ChunkData chunkData = ChunkData.of(DEFAULT_LONG, DEFAULT_LONG,
                DEFAULT_LONG, version, chunkPayload.getBytes());
        chunkData = encodingProcessor.process(chunkData);
        final ByteSequence payload = chunkData.getPayload();
        final ChunkHeader header = ChunkHeader.of(chunkData.getMagicNumber(),
                chunkData.getVersion(), payload.length(), chunkData.getCrc(),
                chunkData.getFlags());
        final ByteSequence headerSequence = header.getBytes();
        final ByteSequence combined = ConcatenatedByteSequence
                .of(headerSequence, payload);
        final ByteSequence padded = padToCellSize(combined,
                CellPosition.CELL_SIZE);
        return cellStoreWriter.write(padded);
    }

    private static ByteSequence padToCellSize(final ByteSequence sequence,
            final int cellSize) {
        final int remainder = sequence.length() % cellSize;
        if (remainder == 0) {
            return sequence;
        }
        final int padding = cellSize - remainder;
        return ConcatenatedByteSequence.of(sequence,
                new ZeroByteSequence(padding));
    }

    private static final class ZeroByteSequence implements ByteSequence {

        private final int length;

        private ZeroByteSequence(final int length) {
            this.length = length;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public byte getByte(final int index) {
            if (index < 0 || index >= length) {
                throw new IllegalArgumentException(String.format(
                        "Property 'index' must be between 0 and %d (inclusive). Got: %d",
                        Math.max(length - 1, 0), index));
            }
            return 0;
        }

        @Override
        public void copyTo(final int sourceOffset, final byte[] target,
                final int targetOffset, final int len) {
            Vldtn.requireNonNull(target, "target");
            if (sourceOffset < 0 || len < 0 || sourceOffset + len > length
                    || targetOffset < 0 || targetOffset + len > target.length) {
                throw new IllegalArgumentException(
                        "Invalid copy range for zero padding");
            }
            if (len == 0) {
                return;
            }
            java.util.Arrays.fill(target, targetOffset, targetOffset + len,
                    (byte) 0);
        }

        @Override
        public ByteSequence slice(final int fromInclusive,
                final int toExclusive) {
            if (fromInclusive < 0 || toExclusive < fromInclusive
                    || toExclusive > length) {
                throw new IllegalArgumentException(
                        "Invalid slice range for zero padding");
            }
            final int newLength = toExclusive - fromInclusive;
            if (newLength == 0) {
                return Bytes.EMPTY;
            }
            return new ZeroByteSequence(newLength);
        }
    }

}
