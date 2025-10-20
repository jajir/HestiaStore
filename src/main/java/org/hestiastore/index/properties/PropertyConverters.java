package org.hestiastore.index.properties;

import org.hestiastore.index.IndexException;

final class PropertyConverters {

    String toString(final Object value) {
        if (value == null) {
            throw new IndexException("Provided property value is null.");
        }
        return String.valueOf(value);
    }

    int toInt(final String value) {
        return value == null ? 0 : Integer.parseInt(value);
    }

    long toLong(final String value) {
        return value == null ? 0L : Long.parseLong(value);
    }

    double toDouble(final String value) {
        return value == null ? 0D : Double.parseDouble(value);
    }

    boolean toBoolean(final String value) {
        return value != null && Boolean.parseBoolean(value);
    }
}
