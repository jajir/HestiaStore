package org.hestiastore.index.properties;

/**
 * Mutating writer that stages property changes before they are persisted by a
 * {@link PropertyTransaction}.
 */
public interface PropertyWriter {

    PropertyWriter setString(String propertyKey, String value);

    PropertyWriter setInt(String propertyKey, int value);

    PropertyWriter setLong(String propertyKey, long value);

    PropertyWriter setDouble(String propertyKey, double value);

    PropertyWriter setBoolean(String propertyKey, boolean value);
}
