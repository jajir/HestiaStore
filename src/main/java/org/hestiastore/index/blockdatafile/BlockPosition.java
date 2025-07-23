package org.hestiastore.index.blockdatafile;

/**
 * Alows to identify exact position of a block in a block data file.
 */
public class BlockPosition {

    private final int position;

    public static BlockPosition of(final int position) {
        return new BlockPosition(position);
    }

    private BlockPosition(final int position) {
        if (position < 0) {
            throw new IllegalArgumentException("Position must be non-negative");
        }
        this.position = position;
    }

    public int getPosition() {
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

        BlockPosition that = (BlockPosition) o;
        return position == that.position;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(position);
    }

    @Override
    public String toString() {
        return "BlockPosition{" + "position=" + position + '}';
    }
}
