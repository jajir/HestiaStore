package org.hestiastore.index;

/**
 * Interface for write transactions that allow writing key-value pairs and
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
     * Opens a writer for writing key-value pairs.
     *
     * @return a PairWriter instance for writing pairs
     */
    PairWriter<K, V> openWriter();

    /**
     * Method execute write operation. It can be used directly of as pattern how
     * to maually call it. Disadvantage of this method is that it complicate
     * junit tests.
     *
     * @param writeFunction the function to apply
     */
    default void execute(final WriterFunction<K, V> writeFunction) {
        try (PairWriter<K, V> writer = openWriter()) {
            writeFunction.apply(writer);
        }
        commit();
    }

    /**
     * Function that is used to write pairs to the transaction.
     */
    @FunctionalInterface
    interface WriterFunction<K, V> {
        void apply(PairWriter<K, V> writer);
    }
}
