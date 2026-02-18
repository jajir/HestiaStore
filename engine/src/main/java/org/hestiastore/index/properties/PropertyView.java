package org.hestiastore.index.properties;

/**
 * Read-only view over a property set with typed accessors.
 */
public interface PropertyView {

    String getString(String propertyKey);

    default String getStringOrDefault(final String propertyKey,
            final String defaultValue) {
        final String value = getString(propertyKey);
        return value == null ? defaultValue : value;
    }

    int getInt(String propertyKey);

    long getLong(String propertyKey);

    double getDouble(String propertyKey);

    boolean getBoolean(String propertyKey);
}
