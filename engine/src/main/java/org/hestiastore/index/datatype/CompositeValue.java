package org.hestiastore.index.datatype;

import java.util.Arrays;

import org.hestiastore.index.Vldtn;

/**
 * Represents multiple values of different types. It doesn't support compile
 * time data type validation. There is just run time type validation when value
 * is stored.
 */
public class CompositeValue {

    private final Object[] elements;

    /**
     * Creates a composite value from supplied elements.
     *
     * @param elements ordered element values
     * @return created value
     */
    public static CompositeValue of(Object... elements) {
        return new CompositeValue(elements);
    }

    /**
     * Creates a composite value by defensively copying supplied elements.
     *
     * @param elements ordered element values
     */
    public CompositeValue(Object... elements) {
        Vldtn.requireNonNull(elements, "elements");
        this.elements = Arrays.copyOf(elements, elements.length);
    }

    /**
     * Returns all elements as a defensive copy.
     *
     * @return copied element array
     */
    public Object[] getElements() {
        return Arrays.copyOf(elements, elements.length);
    }

    /**
     * Returns element at the given index.
     *
     * @param index zero-based index
     * @return element value
     */
    public Object get(int index) {
        return elements[index];
    }

    /**
     * Returns number of elements.
     *
     * @return element count
     */
    public int size() {
        return elements.length;
    }

    /**
     * Compares composite values by element-array equality.
     *
     * @param obj object to compare
     * @return {@code true} when elements are equal
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CompositeValue))
            return false;
        return Arrays.equals(elements, ((CompositeValue) obj).elements);
    }

    /**
     * Returns hash code of elements.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(elements);
    }

    /**
     * Returns string representation of elements.
     *
     * @return diagnostic string
     */
    @Override
    public String toString() {
        return Arrays.toString(elements);
    }

}
