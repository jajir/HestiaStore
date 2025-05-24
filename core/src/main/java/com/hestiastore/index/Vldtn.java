package com.hestiastore.index;

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

}
