package org.hestiastore.index.datatype;

/**
 * Marker value used by {@link TypeDescriptorNull}.
 *
 * <p>
 * The class exposes two singleton instances:
 * {@link #NULL} for regular value payloads and {@link #TOMBSTONE} as a delete
 * sentinel.
 * </p>
 */
public class NullValue {

    /**
     * Singleton instance representing regular null-like value.
     */
    public static final NullValue NULL = new NullValue();

    /**
     * Singleton instance representing tombstone marker.
     */
    public static final NullValue TOMBSTONE = new NullValue() {
        @Override
        public int hashCode() {
            return 7;
        }
    };

    private NullValue() {
        // Private constructor to prevent instantiation
        // Use the static instances NULL and TOMBSTONE instead
    }

    /**
     * Compares by marker semantics implemented via {@link #hashCode()}.
     *
     * @param obj object to compare
     * @return {@code true} when both markers have the same semantic identity
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof NullValue)) {
            return false;
        }
        return hashCode() == obj.hashCode();
    }

    /**
     * Returns hash code used to distinguish {@link #NULL} and
     * {@link #TOMBSTONE}.
     *
     * @return marker hash code
     */
    @Override
    public int hashCode() {
        return 3;
    }

    /**
     * Returns marker name.
     *
     * @return string representation
     */
    @Override
    public String toString() {
        return "NullValue";
    }

}
