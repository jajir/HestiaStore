package org.hestiastore.index.properties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.Vldtn;

public final class PropertyTransaction
        extends GuardedWriteTransaction<PropertyWriterImpl>
        implements AutoCloseable {

    private final Map<String, String> workingCopy;
    private final Properties target;
    private final PropertyStoreimpl store;

    PropertyTransaction(final PropertyStoreimpl store,
            final Properties target) {
        this.store = Vldtn.requireNonNull(store, "store");
        this.target = Vldtn.requireNonNull(target, "target");
        this.workingCopy = new HashMap<>();
        for (final String key : target.stringPropertyNames()) {
            workingCopy.put(key, target.getProperty(key));
        }
    }

    public PropertyWriter openPropertyWriter() {
        return open();
    }

    @Override
    protected PropertyWriterImpl doOpen() {
        return new PropertyWriterImpl(workingCopy);
    }

    @Override
    protected void doCommit(final PropertyWriterImpl resource) {
        resource.close();
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
