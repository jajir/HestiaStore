package org.hestiastore.index.properties;

import java.util.Map;

import org.hestiastore.index.AbstractCloseableResource;

final class PropertyWriterImpl extends AbstractCloseableResource implements PropertyWriter {

    private final Map<String, String> workingCopy;
    private final PropertyConverters converters = new PropertyConverters();

    PropertyWriterImpl(final Map<String, String> workingCopy) {
        this.workingCopy = workingCopy;
    }

    @Override
    public PropertyWriter setString(final String propertyKey,
            final String value) {
        workingCopy.put(propertyKey, value);
        return this;
    }

    @Override
    public PropertyWriter setInt(final String propertyKey, final int value) {
        return setString(propertyKey, converters.formatInt(value));
    }

    @Override
    public PropertyWriter setLong(final String propertyKey, final long value) {
        return setString(propertyKey, converters.formatLong(value));
    }

    @Override
    public PropertyWriter setDouble(final String propertyKey,
            final double value) {
        return setString(propertyKey, converters.formatDouble(value));
    }

    @Override
    public PropertyWriter setBoolean(final String propertyKey,
            final boolean value) {
        return setString(propertyKey, String.valueOf(value));
    }

    @Override
    protected void doClose() {
        // no-op, provided for transactional compatibility
    }
}
