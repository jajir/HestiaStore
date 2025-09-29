package org.hestiastore.index.datablockfile;

/**
 * Identify exact position of a block in a block data file.
 */
public class DataBlockPosition {

    private final int position;

    /**
     * Create data block position.
     * 
     * @param position data block position in bytes
     * @return data block position
     */
    public static DataBlockPosition of(final int position) {
        return new DataBlockPosition(position);
    }

    private DataBlockPosition(final int position) {
        if (position < 0) {
            throw new IllegalArgumentException("Position must be non-negative");
        }
        this.position = position;
    }

    /**
     * Provide data block position in bytes.
     * 
     * @return data block position in bytes
     */
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
