package org.hestiastore.index;

/**
 * A single step in a filter chain.
 *
 * @param <Context> immutable input carried through the chain
 * @param <Result> mutable accumulator updated by each filter
 */
public interface Filter<Context, Result> {

    /**
     * Applies this filter. Return {@code true} to continue with the next filter,
     * or {@code false} to short-circuit the chain.
     */
    boolean filter(Context context, Result result);

}
