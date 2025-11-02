package org.hestiastore.index.chunkstore;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.Vldtn;

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
    public CellPosition write(final ByteSequence bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        Vldtn.requireCellSize(bytes.length(), "bytes");
        final CellPosition returnPosition = cursor.getNextCellPosition();
        final int totalLength = bytes.length();
        int offset = 0;
        while (offset < totalLength) {
            final int availableBytes = cursor.getAvailableBytes();
            if (availableBytes <= 0) {
                throw new IllegalStateException(
                        "Cursor reported no available bytes to write.");
            }
            final int chunkSize = Math.min(availableBytes,
                    totalLength - offset);
            if (chunkSize <= 0) {
                throw new IllegalStateException(
                        "Calculated chunk size must be greater than zero.");
            }
            final ByteSequence chunk = bytes.slice(offset, offset + chunkSize);
            cursor.write(chunk);
            offset += chunkSize;
        }
        return returnPosition;
    }

    @Override
    protected void doClose() {
        cursor.close();
    }

}
