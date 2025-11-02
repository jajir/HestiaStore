package org.hestiastore.index.chunkstore;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.Bytes;
import org.hestiastore.index.bytes.ConcatenatedByteSequence;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockPayload;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datablockfile.DataBlockWriter;

/**
 * Cursor for writing to a CellStore. It provides current data block to write.
 * Check consistency and and finally write data to data block store.
 */
public final class CellStoreWriterCursor extends AbstractCloseableResource {

    private final DataBlockWriter dataBlockWriter;
    private final int dataBlockPayloadSize;
    private CellPosition currentCellPosition;
    private ByteSequence currentDataBlock = null;

    /**
     * Create a new CellStoreWriterCursor.
     *
     * @param dataBlockWriter  required writer to write data blocks
     * @param blockPayloadSize required size of the data block payload
     */
    public CellStoreWriterCursor(final DataBlockWriter dataBlockWriter,
            final DataBlockSize blockPayloadSize) {
        this.dataBlockWriter = Vldtn.requireNonNull(dataBlockWriter,
                "dataBlockWriter");
        Vldtn.requireNonNull(blockPayloadSize, "blockPayloadSize");
        this.dataBlockPayloadSize = blockPayloadSize.getPayloadSize();
        this.currentCellPosition = CellPosition.of(blockPayloadSize, 0);
    }

    /**
     * Write bytes to the current data block. If there is not enough space in
     * the current data block, it will throw an exception.
     *
     * @param bytes required bytes representings set of cells
     * @return position where will be written next cells
     */
    public CellPosition write(final ByteSequence bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        Vldtn.requireCellSize(bytes.length(), "bytes");

        final int availableBytes = getAvailableBytes();
        if (availableBytes < bytes.length()) {
            throw new IllegalStateException(
                    "Not enough space to write to data block");
        }

        appendToCurrentDataBlock(bytes);
        if (currentDataBlock.length() == dataBlockPayloadSize) {
            dataBlockWriter.write(DataBlockPayload.of(currentDataBlock));
            currentDataBlock = null;
            currentCellPosition = currentCellPosition
                    .addCellsForBytes(bytes.length());
        } else {
            currentCellPosition = currentCellPosition
                    .addCellsForBytes(bytes.length());
        }

        return currentCellPosition;
    }

    @Override
    protected void doClose() {
        if (currentDataBlock != null) {
            if (getAvailableBytes() > 0) {
                final ByteSequence padding = Bytes
                        .of(new byte[getAvailableBytes()]);
                final ByteSequence paddedBlock = ConcatenatedByteSequence
                        .of(currentDataBlock, padding);
                dataBlockWriter.write(DataBlockPayload.of(paddedBlock));
            } else {
                throw new IllegalStateException(
                        "Data block is full, should have been written already");
            }
            currentDataBlock = null;
        }
        dataBlockWriter.close();
    }

    /**
     * Get number of available bytes in the current data block.
     *
     * @return number of available bytes in the current data block
     */
    public int getAvailableBytes() {
        return currentCellPosition.getFreeBytesInCurrentDataBlock();
    }

    /**
     * Get position where will be written next cells.
     *
     * @return position where will be written next cells
     */
    public CellPosition getNextCellPosition() {
        return currentCellPosition;
    }

    private void appendToCurrentDataBlock(final ByteSequence bytes) {
        if (currentDataBlock == null) {
            currentDataBlock = bytes;
            return;
        }
        currentDataBlock = ConcatenatedByteSequence.of(currentDataBlock, bytes);
    }

}
