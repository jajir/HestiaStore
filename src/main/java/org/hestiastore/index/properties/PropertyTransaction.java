package org.hestiastore.index.properties;

/**
 * Transaction for mutating property changes that must propagate updates via a
 * dedicated {@link PropertyWriter}.
 */
public interface PropertyTransaction extends AutoCloseable {

    /**
     * Opens a writer that stages updates to the underlying property map.
     */
    PropertyWriter openPropertyWriter();

    /**
     * Persists any pending changes. Implementations must be idempotent so that
     * repeated invocations have no additional effect. Calling this method is
     * optional; omitting it leaves the staged changes unapplied.
     */
    @Override
    void close();
}
