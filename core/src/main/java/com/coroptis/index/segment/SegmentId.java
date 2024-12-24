package com.coroptis.index.segment;

import java.util.Objects;

/**
 * Index segments consisting of Sorted String Table (sst).
 * 
 * @author honza
 *
 */
public class SegmentId {

    private final int id;

    /**
     * Hidden constructor.
     * 
     * @param id required segment id.
     */
    private SegmentId(final int id) {
        this.id = id;
    }

    public static SegmentId of(final int id) {
        return new SegmentId(id);
    }

    public int getId() {
        return id;
    }

    /**
     * It will be used as part of segment files.
     * 
     * @return return segment name
     */
    public String getName() {
        String name = String.valueOf(id);
        while (name.length() < 5) {
            name = "0" + name;
        }
        return "segment-" + name;
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public int hashCode() {
        return id;
    }

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
