package org.hestiastore.index.properties;

/**
 * Typed property store abstraction that supports transactional updates.
 */
public interface PropertyStore {

    PropertyTransaction beginTransaction();

    /**
     * Opens a mutation session that can apply multiple changes in memory and
     * persist them with a single write on close.
     *
     * @return opened mutation session
     */
    PropertyMutationSession openMutationSession();

    /**
     * Returns a read-only snapshot of the current property set.
     */
    PropertyView snapshot();

}
