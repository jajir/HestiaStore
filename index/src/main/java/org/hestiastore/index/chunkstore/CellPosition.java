package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockPosition;
import org.hestiastore.index.datablockfile.DataBlockSize;

/**
 * Specify position within a chunk store and specify particular chunk.
 */
public class CellPosition {

    /**
     * Cell is smallest addressable unit in data block. Cell size is 16 bytes.
     * 
     */
    public static final int CELL_SIZE = 16;

    private final DataBlockSize dataBlockSize;

    private final int position;

    /**
     * Create position within chunk store.
     * 
     * @param dataBlockSize Data block size
     * @param position      Position in bytes. Must be non-negative.
     * @return Position within chunk store.
     */
    public static CellPosition of(final DataBlockSize dataBlockSize,
            final int position) {
        return new CellPosition(dataBlockSize, position);
    }

    private CellPosition(final DataBlockSize dataBlockSize,
            final int position) {
        if (position < 0) {
            throw new IllegalArgumentException("Position must be non-negative");
        }
        this.position = position;
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
    }

    /**
     * Get position in bytes.
     * 
     * @return Position in bytes.
     */
    public int getValue() {
        return position;
    }

    /**
     * Get data block payload size in bytes.
     * 
     * @return Data block payload size in bytes.
     */
    int getDataBlockPayloadSize() {
        return dataBlockSize.getPayloadSize();
    }

    /**
     * Get position of the start of data block containing this position.
     * 
     * @return Position of the start of data block containing this position.
     */
    public DataBlockPosition getDataBlockStartPosition() {
        final int blockIndex = position / dataBlockSize.getPayloadSize();
        return DataBlockPosition
                .of(blockIndex * dataBlockSize.getDataBlockSize());
    }

    /**
     * Get number of free bytes in current data block.
     * 
     * @return Number of free bytes in current data block.
     */
    public int getFreeBytesInCurrentDataBlock() {
        return getDataBlockPayloadSize() - position % getDataBlockPayloadSize();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        if (this == o) {
            return true;
        }

        if (getClass() != o.getClass()) {
            return false;
        }

        CellPosition that = (CellPosition) o;
        return position == that.position;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(position);
    }

    @Override
    public String toString() {
        return "ChunkStorePosition{" + "value=" + position + '}';
    }

    /**
     * Add one data block size to current position.
     * 
     * @return New position advanced by one data block size.
     */
    public CellPosition addDataBlock() {
        return CellPosition.of(dataBlockSize,
                position + dataBlockSize.getDataBlockSize());
    }

    /**
     * Add cells to current position. If byte count is not multiple of cell
     * size, it is rounded up.
     * 
     * @param byteCount Number of bytes to add.
     * @return New position advanced by given number of bytes (rounded up to
     *         whole cells).
     */
    public CellPosition addCellsForBytes(final int byteCount) {
        int cells = byteCount / CELL_SIZE;
        if (byteCount % CELL_SIZE != 0) {
            cells++;
        }
        return CellPosition.of(dataBlockSize, position + cells * CELL_SIZE);
    }

    private int getStartingByteInBlockOfCell() {
        return position % getDataBlockPayloadSize();
    }

    /**
     * Get index of cell within data block.
     * 
     * @return SegmentIndex of cell within data block.
     */
    public int getCellIndex() {
        return (getStartingByteInBlockOfCell()) / CELL_SIZE;
    }

    /**
     * Where starts empty space in data block.
     * 
     * @return
     */
    public int getOccupiedBytes() {
        return getCellIndex() * CELL_SIZE;
    }

}
