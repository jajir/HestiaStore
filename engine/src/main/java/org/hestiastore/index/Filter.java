package org.hestiastore.index;

/**
 * A single step in a filter chain.
 *
 * @param <C> immutable input carried through the chain
 * @param <R> mutable accumulator updated by each filter
 */
public interface Filter<C, R> {

    /**
     * Applies this filter. Return {@code true} to continue with the next filter,
     * or {@code false} to short-circuit the chain.
     */
    boolean filter(C context, R result);

}
