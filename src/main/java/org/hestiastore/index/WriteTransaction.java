package org.hestiastore.index;

/**
 * Interface for write transactions that allow writing key-value pairs and
 * committing changes.
 *
 * Interface allows easily and securely perform write operations including
 * closing all resources and committing.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public interface WriteTransaction<K, V> {

    /**
     * Opens a writer for writing key-value pairs.
     *
     * @return a PairWriter instance for writing pairs
     */
    PairWriter<K, V> openWriter();

    /**
     * Commits the changes made during the transaction. This method is called
     * after successfully writing all pairs.
     */
    void commit();

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
        } catch (Exception e) {
            throw new IndexException(
                    "Error during writing pairs: " + e.getMessage(), e);
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
