package org.hestiastore.index.properties;

/**
 * Typed property store abstraction that supports transactional updates.
 */
public interface PropertyStore {

    PropertyTransaction beginTransaction();

    /**
     * Returns a read-only snapshot of the current property set.
     */
    PropertyView snapshot();

}
