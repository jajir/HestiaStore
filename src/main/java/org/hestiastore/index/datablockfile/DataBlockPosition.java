package org.hestiastore.index.datablockfile;

/**
 * Alows to identify exact position of a block in a block data file.
 */
public class DataBlockPosition {

    private final int position;

    public static DataBlockPosition of(final int position) {
        return new DataBlockPosition(position);
    }

    private DataBlockPosition(final int position) {
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

        DataBlockPosition that = (DataBlockPosition) o;
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
