package org.hestiastore.index.properties;

import java.util.Map;

final class PropertyViewImpl implements PropertyView {

    private final Map<String, String> delegate;
    private final PropertyConverters converters;

    PropertyViewImpl(final Map<String, String> delegate,
            final PropertyConverters converters) {
        this.delegate = delegate;
        this.converters = converters;
    }

    @Override
    public String getString(final String propertyKey) {
        return delegate.get(propertyKey);
    }

    @Override
    public int getInt(final String propertyKey) {
        return converters.toInt(delegate.get(propertyKey));
    }

    @Override
    public long getLong(final String propertyKey) {
        return converters.toLong(delegate.get(propertyKey));
    }

    @Override
    public double getDouble(final String propertyKey) {
        return converters.toDouble(delegate.get(propertyKey));
    }

    @Override
    public boolean getBoolean(final String propertyKey) {
        return converters.toBoolean(delegate.get(propertyKey));
    }
}
