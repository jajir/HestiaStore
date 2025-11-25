package org.hestiastore.index;

import java.util.Collection;

/**
 * Class provide validation methods. It ensure that error message is easy to
 * understand and consistent.
 */
public final class Vldtn {

    private Vldtn() {
        // private constructor
    }

    /**
     * Validates that the provided object is not null.
     * 
     * @param <T>          the type of the object
     * @param object       the object to validate
     * @param propertyName the name of the property being validated, used in
     *                     error messages
     * @return the validated object
     * @throws IllegalArgumentException if the object is null or propertyName is
     *                                  null
     */
    public static <T> T requireNonNull(final T object,
            final String propertyName) {
        if (propertyName == null) {
            throw new IllegalArgumentException(
                    "Property 'propertyName' must not be null.");
        }
        if (object == null) {
            throw new IllegalArgumentException(String
                    .format("Property '%s' must not be null.", propertyName));
        }
        return object;
    }

    /**
     * Validates that a given IO buffer size is greater than 0 and divisible by
     * 1024.
     *
     * @param ioBufferSize the IO buffer size to validate
     * @return the validated IO buffer size
     * @throws IllegalArgumentException if the IO buffer size is less than or
     *                                  equal to 0, or not divisible by 1024
     */
    public static int requireIoBufferSize(final int ioBufferSize) {
        if (ioBufferSize <= 0) {
            throw new IllegalArgumentException(
                    "Property 'ioBufferSize' must be greater than 0");
        }
        if (ioBufferSize % 1024 != 0) {
            throw new IllegalArgumentException(String.format(
                    "Propety 'ioBufferSize' must be divisible by 1024 "
                            + "(e.g., 1024, 2048, 4096). Got: '%s'",
                    ioBufferSize));
        }
        return ioBufferSize;
    }

    /**
     * Validates that a given integer value is between a minimum and maximum
     * value (inclusive).
     *
     * @param value        the value to validate
     * @param minInclusive the minimum allowed value (inclusive)
     * @param maxInclusive the maximum allowed value (inclusive)
     * @param propertyName the name of the property being validated, used in
     *                     error messages
     * @return the validated value if it is within range
     * @throws IllegalArgumentException if the value is out of range or
     *                                  propertyName is null
     */
    public static int requireBetween(final int value, final int minInclusive,
            final int maxInclusive, final String propertyName) {
        if (propertyName == null) {
            throw new IllegalArgumentException(
                    "Property 'propertyName' must not be null.");
        }
        if (value < minInclusive || value > maxInclusive) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must be between %d and %d (inclusive). Got: %d",
                    propertyName, minInclusive, maxInclusive, value));
        }
        return value;
    }

    /**
     * Validates that given value is valid cell size
     * 
     * @param value        cell size to validate
     * @param propertyName name of the property being validated, used in error
     *                     messages
     * @throws IllegalArgumentException if cellSize is less than or equal to 0,
     *                                  or not divisible by 16
     * @return the validated cell size
     */
    public static int requireGreaterThanZero(final int value,
            final String propertyName) {
        if (propertyName == null) {
            throw new IllegalArgumentException(
                    "Property 'propertyName' must not be null.");
        }
        if (value <= 0) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must be greater than 0", propertyName));
        }
        return value;
    }

    /**
     * Validates that the provided long value is greater than zero.
     *
     * @param value        long value to validate
     * @param propertyName name of the property for error reporting
     * @return the original value when validation passes
     * @throws IllegalArgumentException if {@code propertyName} is null or value
     *                                  is not greater than zero
     */
    public static long requireGreaterThanZero(final long value,
            final String propertyName) {
        if (propertyName == null) {
            throw new IllegalArgumentException(
                    "Property 'propertyName' must not be null.");
        }
        if (value <= 0) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must be greater than 0", propertyName));
        }
        return value;
    }

    /**
     * Validates that the provided long value is strictly less than the given
     * upper bound.
     *
     * @param value        the value to validate
     * @param maxExclusive exclusive upper bound
     * @param propertyName name of the property for error reporting
     * @return the original value when validation passes
     * @throws IllegalArgumentException if {@code propertyName} is null or value
     *                                  is not less than {@code maxExclusive}
     */
    public static long requireLessThan(final long value,
            final long maxExclusive, final String propertyName) {
        if (propertyName == null) {
            throw new IllegalArgumentException(
                    "Property 'propertyName' must not be null.");
        }
        if (value >= maxExclusive) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must be less than %d", propertyName,
                    maxExclusive));
        }
        return value;
    }

    private static final int CELL_SIZE = 16;

    /**
     * Validates that given value is valid cell size
     * 
     * @param cellSize     cell size to validate
     * @param propertyName name of the property being validated, used in error
     *                     messages
     * @throws IllegalArgumentException if cellSize is less than or equal to 0,
     *                                  or not divisible by 16
     * @return the validated cell size
     */
    public static int requireCellSize(final int cellSize,
            final String propertyName) {
        if (propertyName == null) {
            throw new IllegalArgumentException(
                    "Property 'propertyName' must not be null.");
        }
        if (cellSize <= 0) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must be greater than 0", propertyName));
        }
        if (cellSize % CELL_SIZE != 0) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must be divisible by %d (e.g., %d, %d, %d). Got: '%s'",
                    propertyName, CELL_SIZE, CELL_SIZE, CELL_SIZE * 2,
                    CELL_SIZE * 4, cellSize));
        }
        return cellSize;
    }

    /**
     * Validates that the provided collection is not null and not empty.
     * 
     * @param <T>          the type of elements in the collection
     * @param <C>          the type of the collection
     * @param collection   the collection to validate
     * @param propertyName the name of the property being validated, used in
     *                     error messages
     * @return the validated collection
     * @throws IllegalArgumentException if the collection is null or empty
     */
    public static <T, C extends Collection<T>> C requireNotEmpty(
            final C collection, final String propertyName) {
        final C validatedCollection = requireNonNull(collection, propertyName);
        if (validatedCollection.isEmpty()) {
            throw new IllegalArgumentException(String
                    .format("Property '%s' must not be empty.", propertyName));
        }
        return validatedCollection;
    }

}
