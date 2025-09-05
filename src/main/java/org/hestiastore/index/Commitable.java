package org.hestiastore.index;

/**
 * Defines a contract for commit operations in transactions.
 */
public interface Commitable {

    /**
     * Commits the changes made during the transaction. This method is called
     * after successfully writing all pairs.
     */
    void commit();

}
