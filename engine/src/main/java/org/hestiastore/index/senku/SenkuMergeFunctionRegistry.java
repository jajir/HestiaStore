package org.hestiastore.index.senku;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

/**
 * Holds the single duplicate-value merge function used by one writing index.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SenkuMergeFunctionRegistry<K, V> {

    private SenkuMergeFunction<K, V> function;
    private boolean frozen;

    /**
     * Creates an empty registry.
     */
    public SenkuMergeFunctionRegistry() {
        // The function is registered explicitly before index creation.
    }

    /**
     * Registers the index's only merge function.
     *
     * @param mergeFunction required merge function
     * @return this registry
     * @throws IndexException if a function is already registered
     */
    public SenkuMergeFunctionRegistry<K, V> register(
            final SenkuMergeFunction<K, V> mergeFunction) {
        final SenkuMergeFunction<K, V> validatedFunction = Vldtn
                .requireNonNull(mergeFunction, "mergeFunction");
        if (frozen) {
            throw new IndexException("Senku merge function registry is frozen.");
        }
        if (function != null) {
            throw new IndexException(
                    "Senku merge function registry accepts exactly one function.");
        }
        function = validatedFunction;
        return this;
    }

    SenkuMergeFunction<K, V> requireFunction() {
        if (function == null) {
            throw new IndexException(
                    "Senku merge function registry requires exactly one function.");
        }
        return function;
    }

    void freeze() {
        requireFunction();
        frozen = true;
    }
}
