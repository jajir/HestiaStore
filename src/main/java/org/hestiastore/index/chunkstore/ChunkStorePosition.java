package org.hestiastore.index.chunkstore;

import org.hestiastore.index.blockdatafile.BlockPosition;

/**
 * Specifi position within a chunk store.
 */
public class ChunkStorePosition {

    private final int position;

    public static ChunkStorePosition of(int position) {
        return new ChunkStorePosition(position);
    }

    private ChunkStorePosition(int position) {
        if (position < 0) {
            throw new IllegalArgumentException("Position must be non-negative");
        }
        this.position = position;
    }

    public int getValue() {
        return position;
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

    public BlockPosition toDataBlockPosition() {
        return BlockPosition.of(position >> 4);
    }

    public int cellIndex() {
        return position % Chunk.CELL_SIZE;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(position);
    }

    @Override
    public String toString() {
        return "ChunkStorePosition{" + "position=" + position + '}';
    }

}
