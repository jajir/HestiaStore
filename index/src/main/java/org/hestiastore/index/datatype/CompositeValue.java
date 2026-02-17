package org.hestiastore.index.datatype;

import java.util.Arrays;

/**
 * Represents multiple values of different types. It doesn't support compile
 * time data type validation. There is just run time type validation when value
 * is stored.
 */
public class CompositeValue {

    private final Object[] elements;

    public static CompositeValue of(Object... elements) {
        return new CompositeValue(elements);
    }

    public CompositeValue(Object... elements) {
        this.elements = elements;
    }

    public Object[] getElements() {
        return elements;
    }

    public Object get(int index) {
        return elements[index];
    }

    public int size() {
        return elements.length;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CompositeValue))
            return false;
        return Arrays.equals(elements, ((CompositeValue) obj).elements);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(elements);
    }

    @Override
    public String toString() {
        return Arrays.toString(elements);
    }

}
