package org.hestiastore.index;

/**
 * Interface for write transactions that allow writing key-value entries and
 * committing changes.
 *
 * Interface allows easily and securely perform write operations including
 * closing all resources and committing.
 * 
 * Commit can't be part of close() method.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public interface WriteTransaction<K, V> extends Commitable {

    /**
     * Opens a writer for writing key-value entries.
     *
     * @return a EntryWriter instance for writing entries
     */
    default EntryWriter<K, V> open() {
        if (this instanceof GuardedWriteTransaction) {
            @SuppressWarnings("unchecked")
            final GuardedWriteTransaction<EntryWriter<K, V>> transaction = (GuardedWriteTransaction<EntryWriter<K, V>>) this;
            return transaction.open();
        }
        throw new UnsupportedOperationException(
                "Implementations must override open()");
    }

    /**
     * Method execute write operation. It can be used directly of as pattern how
     * to maually call it. Disadvantage of this method is that it complicate
     * junit tests.
     *
     * @param writeFunction the function to apply
     */
    default void execute(final WriterFunction<K, V> writeFunction) {
        try (EntryWriter<K, V> writer = open()) {
            writeFunction.apply(writer);
        }
        commit();
    }

    /**
     * Function that is used to write entries to the transaction.
     */
    @FunctionalInterface
    interface WriterFunction<K, V> {
        void apply(EntryWriter<K, V> writer);
    }
}
