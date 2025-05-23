package com.coroptis.index;

/**
 * Class provide validation methods. It ensure that error message is easy to
 * understand and consistent.
 */
public final class Vldtn {

    private Vldtn() {
        // private constructor
    }

    public static <T> T requiredNotNull(final T object,
            final String propertyName) {
        if (propertyName == null) {
            throw new IllegalArgumentException(
                    "Proepty 'propertyName' must not be null");
        }
        if (object == null) {
            throw new IllegalArgumentException(String
                    .format("Property ‘%s’ must not be null.", propertyName));
        }
        return object;
    }

}
