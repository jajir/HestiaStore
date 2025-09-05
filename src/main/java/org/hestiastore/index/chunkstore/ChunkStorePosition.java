package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockPosition;
import org.hestiastore.index.datablockfile.DataBlock;

/**
 * Specifi position within a chunk store.
 */
public class ChunkStorePosition {

    private final int dataBlockSize;

    private final int position;

    public static ChunkStorePosition of(final int dataBlockSize,
            final int position) {
        return new ChunkStorePosition(dataBlockSize, position);
    }

    private ChunkStorePosition(final int dataBlockSize, final int position) {
        if (position < 0) {
            throw new IllegalArgumentException("Position must be non-negative");
        }
        this.position = position;
        this.dataBlockSize = Vldtn.requireCellSize(dataBlockSize,
                "dataBlockSize");
    }

    public int getValue() {
        return position;
    }

    private int getDataBlockPayloadSize() {
        return dataBlockSize - DataBlock.HEADER_SIZE;
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

        ChunkStorePosition that = (ChunkStorePosition) o;
        return position == that.position;
    }

    public DataBlockPosition getDataBlockPosition() {
        return DataBlockPosition.of(position / dataBlockSize);
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(position);
    }

    @Override
    public String toString() {
        return "ChunkStorePosition{" + "value=" + position + '}';
    }

    public ChunkStorePosition addDataBlock() {
        return ChunkStorePosition.of(dataBlockSize, position + dataBlockSize);
    }

    public ChunkStorePosition addCellsForBytes(final int byteCount) {
        int cells = byteCount / Chunk.CELL_SIZE;
        if (byteCount % Chunk.CELL_SIZE != 0) {
            cells++;
        }
        return ChunkStorePosition.of(dataBlockSize,
                position + cells * Chunk.CELL_SIZE);
    }

    public int getCellIndex() {
        return (position % getDataBlockPayloadSize()) / Chunk.CELL_SIZE;
    }

    /**
     * Where starts empty space in data block.
     * 
     * @return
     */
    public int getOccupiedBytes() {
        return getCellIndex() * Chunk.CELL_SIZE;
    }

}
