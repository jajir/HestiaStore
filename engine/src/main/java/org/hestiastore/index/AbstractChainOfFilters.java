package org.hestiastore.index;

import java.util.List;

/**
 * Base helper for executing an ordered list of filters.
 * <p>
 * Subclasses supply the immutable context and mutable result, then delegate to
 * {@link #filter(Object, Object)} to drive the chain. Execution stops when a
 * filter returns {@code false}, enabling short-circuit behavior.
 *
 * @param <Context> immutable input available to every filter
 * @param <Result>  mutable accumulator updated by filters
 */
public abstract class AbstractChainOfFilters<Context, Result> {

    private final List<Filter<Context, Result>> filters;

    /**
     * Constructs a chain of filters executed in order.
     *
     * @param filters ordered list of filters to run
     */
    public AbstractChainOfFilters(final List<Filter<Context, Result>> filters) {
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
    protected boolean filter(final Context context, final Result result) {
        for (final Filter<Context, Result> filter : filters) {
            if (!filter.filter(context, result)) {
                return false;
            }
        }
        return true;
    }

}
