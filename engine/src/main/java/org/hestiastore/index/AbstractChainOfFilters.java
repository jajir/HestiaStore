package org.hestiastore.index;

import java.util.List;

/**
 * Base helper for executing an ordered list of filters.
 * <p>
 * Subclasses supply the immutable context and mutable result, then delegate to
 * {@link #filter(Object, Object)} to drive the chain. Execution stops when a
 * filter returns {@code false}, enabling short-circuit behavior.
 *
 * @param <C> immutable input available to every filter
 * @param <R> mutable accumulator updated by filters
 */
public abstract class AbstractChainOfFilters<C, R> {

    private final List<Filter<C, R>> filters;

    /**
     * Constructs a chain of filters executed in order.
     *
     * @param filters ordered list of filters to run
     */
    protected AbstractChainOfFilters(final List<Filter<C, R>> filters) {
        this.filters = filters;
    }

    /**
     * Iterates over the configured filters until one returns {@code false} or
     * the list is exhausted.
     * 
     * This should be called by subclasses to run the chain. typically in some
     * custom method.
     *
     * @param context immutable context passed to each filter
     * @param result  mutable result accumulator passed to each filter
     * @return {@code true} if all filters ran, {@code false} if short-circuited
     */
    protected boolean filter(final C context, final R result) {
        for (final Filter<C, R> filter : filters) {
            if (!filter.filter(context, result)) {
                return false;
            }
        }
        return true;
    }

}
