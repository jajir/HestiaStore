package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Vldtn;

/**
 * Specify position within a chunk store.
 */
public class CellPosition {

    /**
     * Size of cell in bytes. Cell is smalles addresable unit in chunk store.
     * 
     * TODO this is not correct place for this constant.
     */
    public static final int CELL_SIZE = 16;

    private final DataBlockSize dataBlockSize;

    private final int position;

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

    public int getValue() {
        return position;
    }

    int getDataBlockPayloadSize() {
        return dataBlockSize.getPayloadSize();
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

    public DataBlockPosition getDataBlockStartPosition() {
        final int blockIndex = position / dataBlockSize.getPayloadSize();
        return DataBlockPosition
                .of(blockIndex * dataBlockSize.getDataBlockSize());
    }

    public int getFreeBytesInCurrentDataBlock() {
        return getDataBlockPayloadSize() - position % getDataBlockPayloadSize();
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(position);
    }

    @Override
    public String toString() {
        return "ChunkStorePosition{" + "value=" + position + '}';
    }

    public CellPosition addDataBlock() {
        return CellPosition.of(dataBlockSize,
                position + dataBlockSize.getDataBlockSize());
    }

    public CellPosition addCellsForBytes(final int byteCount) {
        int cells = byteCount / CELL_SIZE;
        if (byteCount % CELL_SIZE != 0) {
            cells++;
        }
        return CellPosition.of(dataBlockSize, position + cells * CELL_SIZE);
    }

    int getStartingByteInBlockOfCell() {
        return position % getDataBlockPayloadSize();
    }

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
