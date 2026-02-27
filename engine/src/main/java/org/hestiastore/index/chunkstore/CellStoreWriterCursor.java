package org.hestiastore.index.chunkstore;

import java.util.Arrays;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.MutableBytes;
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
    private final MutableBytes currentDataBlock;
    private int currentDataBlockPosition = 0;

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
        this.currentDataBlock = MutableBytes.allocate(dataBlockPayloadSize);
    }

    /**
     * Write bytes to the current data block. If there is not enough space in
     * the current data block, it will throw an exception.
     *
     * @param bytes required bytes sequence representing set of cells
     * @return position where will be written next cells
     */
    public CellPosition writeSequence(final ByteSequence bytes) {
        final ByteSequence validated = Vldtn.requireNonNull(bytes, "bytes");
        Vldtn.requireCellSize(validated.length(), "bytes");

        final int availableBytes = getAvailableBytes();
        if (availableBytes < validated.length()) {
            throw new IllegalStateException(
                    "Not enough space to write to data block");
        }

        currentDataBlock.setBytes(currentDataBlockPosition, validated, 0,
                validated.length());
        currentDataBlockPosition += validated.length();
        currentCellPosition = currentCellPosition
                .addCellsForBytes(validated.length());
        if (currentDataBlockPosition == dataBlockPayloadSize) {
            flushCurrentDataBlock();
        }

        return currentCellPosition;
    }

    @Override
    protected void doClose() {
        if (currentDataBlockPosition > 0) {
            zeroOutRemainingDataBlockBytes();
            dataBlockWriter.writeSequence(currentDataBlock);
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

    private void flushCurrentDataBlock() {
        dataBlockWriter.writeSequence(currentDataBlock);
        currentDataBlockPosition = 0;
    }

    private void zeroOutRemainingDataBlockBytes() {
        Arrays.fill(currentDataBlock.array(), currentDataBlockPosition,
                dataBlockPayloadSize, (byte) 0);
    }

}
