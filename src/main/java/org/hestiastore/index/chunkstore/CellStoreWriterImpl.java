package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.CellPosition;

public class CellStoreWriterImpl implements CellStoreWriter {

    private final CellStoreWriterCursor cursor;

    public CellStoreWriterImpl(final CellStoreWriterCursor cursor) {
        this.cursor = Vldtn.requireNonNull(cursor, "cursor");
    }

    @Override
    public CellPosition write(final Bytes bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        Vldtn.requireCellSize(bytes.length(), "bytes");
        final CellPosition returnPosition = cursor.getNextCellPosition();
        Bytes bufferToWrite = bytes.paddedToNextCell();
        while (bufferToWrite != null && bufferToWrite.length() > 0) {
            int availableBytes = cursor.getAvailableBytes();
            int trimTo = Math.min(availableBytes, bufferToWrite.length());
            cursor.write(bufferToWrite.subBytes(0, trimTo));
            bufferToWrite = bufferToWrite.subBytes(trimTo,
                    bufferToWrite.length());
        }
        return returnPosition;
    }

    @Override
    public void close() {
        cursor.close();
    }

}
