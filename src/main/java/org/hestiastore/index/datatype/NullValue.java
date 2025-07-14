package org.hestiastore.index.datatype;

/**
 * Represents a null value in the index data type system. This class serves as a
 * placeholder for null values.
 * 
 * 
 * This is little bit tricky and future problem. Records can't be deleted
 * because NULL value doesn't support tombstones. When user try to store tore
 * tombstone it's converted to NULL value.
 * 
 */
public class NullValue {

    public static final NullValue NULL = new NullValue();
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

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof NullValue)) {
            return false;
        }
        return hashCode() == obj.hashCode();
    }

    @Override
    public int hashCode() {
        return 3;
    }

    @Override
    public String toString() {
        return "NullValue";
    }

}
