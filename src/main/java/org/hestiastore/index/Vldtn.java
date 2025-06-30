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

}
