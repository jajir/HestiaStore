package org.hestiastore.index.chunkstore;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;

/**
 * Implementation of {@link CellStoreWriter}.
 */
public class CellStoreWriterImpl extends AbstractCloseableResource
        implements CellStoreWriter {

    private final CellStoreWriterCursor cursor;

    /**
     * Constructor.
     * 
     * @param cursor required the cursor to use for writing
     */
    public CellStoreWriterImpl(final CellStoreWriterCursor cursor) {
        this.cursor = Vldtn.requireNonNull(cursor, "cursor");
    }

    @Override
    public CellPosition writeSequence(final ByteSequence bytes) {
        final ByteSequence validated = Vldtn.requireNonNull(bytes, "bytes");
        Vldtn.requireCellSize(validated.length(), "bytes");
        final CellPosition returnPosition = cursor.getNextCellPosition();
        int sourceOffset = 0;
        int bytesRemaining = validated.length();
        while (bytesRemaining > 0) {
            int availableBytes = cursor.getAvailableBytes();
            int trimTo = Math.min(availableBytes, bytesRemaining);
            cursor.writeSequence(
                    validated.slice(sourceOffset, sourceOffset + trimTo));
            sourceOffset += trimTo;
            bytesRemaining -= trimTo;
        }
        return returnPosition;
    }

    @Override
    protected void doClose() {
        cursor.close();
    }

}
