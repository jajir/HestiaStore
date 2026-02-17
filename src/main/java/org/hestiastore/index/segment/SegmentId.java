package org.hestiastore.index.segment;

import org.hestiastore.index.FileNameUtil;

/**
 * SegmentIndex segments consisting of Sorted String Table (sst).
 * 
 * @author honza
 *
 */
public final class SegmentId {

    private final int id;

    /**
     * Hidden constructor.
     * 
     * @param id required segment id.
     */
    private SegmentId(final int id) {
        if (id < 0) {
            throw new IllegalArgumentException(
                    "Segment id must be greater than or equal to 0");
        }
        this.id = id;
    }

    /**
     * Creates a new segment id instance.
     *
     * @param id numeric segment id
     * @return segment id wrapper
     */
    public static SegmentId of(final int id) {
        return new SegmentId(id);
    }

    /**
     * Returns the raw numeric id.
     *
     * @return numeric id
     */
    public int getId() {
        return id;
    }

    /**
     * It will be used as part of segment files.
     * 
     * @return return segment name
     */
    public String getName() {
        return "segment-" + FileNameUtil.getPaddedId(id, 5);
    }

    /**
     * Returns the segment name.
     *
     * @return segment name
     */
    @Override
    public String toString() {
        return getName();
    }

    /**
     * Returns hash code based on numeric id.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return id;
    }

    /**
     * Compares two segment ids.
     *
     * @param obj object to compare
     * @return true when ids match
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SegmentId other = (SegmentId) obj;
        return id == other.id;
    }

}
