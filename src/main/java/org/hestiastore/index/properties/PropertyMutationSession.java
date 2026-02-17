package org.hestiastore.index.properties;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Batches multiple property mutations and flushes them with a single
 * transactional write when closed.
 */
public final class PropertyMutationSession implements AutoCloseable {

    private final PropertyTransaction transaction;
    private final PropertyWriter writer;
    private boolean closed;

    PropertyMutationSession(final PropertyTransaction transaction) {
        this.transaction = Objects.requireNonNull(transaction, "transaction");
        this.writer = transaction.openPropertyWriter();
    }

    /**
     * Applies an in-memory mutation to the underlying writer.
     *
     * @param mutator property mutator callback
     * @return this session for chaining
     */
    public PropertyMutationSession mutate(
            final Consumer<PropertyWriter> mutator) {
        ensureOpen();
        Objects.requireNonNull(mutator, "mutator").accept(writer);
        return this;
    }

    /**
     * Returns the writer bound to this mutation session.
     *
     * @return session writer
     */
    public PropertyWriter writer() {
        ensureOpen();
        return writer;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        transaction.close();
    }

    /**
     * Returns true if this session currently contains unsaved modifications.
     *
     * @return true when the working copy differs from the original snapshot
     */
    public boolean hasChanges() {
        ensureOpen();
        return transaction.hasChanges();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Mutation session is closed");
        }
    }
}
