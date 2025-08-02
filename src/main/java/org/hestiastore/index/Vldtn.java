package org.hestiastore.index;

/**
 * Class provide validation methods. It ensure that error message is easy to
 * understand and consistent.
 */
public final class Vldtn {

    private Vldtn() {
        // private constructor
    }

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

    public static int ioBufferSize(final int ioBufferSize) {
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

}
