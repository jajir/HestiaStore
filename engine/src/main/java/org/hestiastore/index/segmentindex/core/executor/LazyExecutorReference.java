package org.hestiastore.index.segmentindex.core.executor;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;

/**
 * Lazily creates an executor on first access and then keeps the same instance
 * for the rest of the owner lifetime.
 *
 * @param <T> executor type
 */
final class LazyExecutorReference<T extends ExecutorService> {

    private final Supplier<T> factory;
    private volatile T executor;

    LazyExecutorReference(final Supplier<T> factory) {
        this.factory = Vldtn.requireNonNull(factory, "factory");
    }

    T get() {
        final T current = executor;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            if (executor == null) {
                executor = Vldtn.requireNonNull(factory.get(), "executor");
            }
            return executor;
        }
    }

    T getIfCreated() {
        return executor;
    }
}
