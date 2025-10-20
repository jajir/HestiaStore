package org.hestiastore.index.properties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

final class PropertyTransactionImpl implements PropertyTransaction {

    private final Map<String, String> workingCopy;
    private final Properties target;
    private final PropertyStoreimpl store;

    PropertyTransactionImpl(final PropertyStoreimpl store,
            final Properties target) {
        this.store = store;
        this.target = target;
        this.workingCopy = new HashMap<>();
        for (final String key : target.stringPropertyNames()) {
            workingCopy.put(key, target.getProperty(key));
        }
    }

    @Override
    public PropertyWriter openPropertyWriter() {
        return new PropertyWriterImpl(workingCopy);
    }

    @Override
    public void close() {
        target.clear();
        for (final Map.Entry<String, String> entry : workingCopy.entrySet()) {
            target.setProperty(entry.getKey(), entry.getValue());
        }
        store.writeToDisk(target);
    }
}
