package org.hestiastore.index.chunkstore;

import java.util.List;

import org.hestiastore.index.Vldtn;

/**
 * Processes {@link ChunkData} through a series of {@link ChunkFilter}s.
 */
public class ChunkProcessor {

    private final List<ChunkFilter> filters;

    public ChunkProcessor(final List<ChunkFilter> filters) {
        final List<ChunkFilter> validated = Vldtn.requireNonNull(filters,
                "filters");
        if (validated.isEmpty()) {
            throw new IllegalArgumentException(
                    "Property 'filters' must not be empty.");
        }
        this.filters = validated;
    }

    /**
     * Processes the given {@link ChunkData} through all configured
     * {@link ChunkFilter}s in order.
     *
     * @param data The chunk data to process; must not be {@code null}.
     * @return The processed chunk data; never {@code null}.
     * @throws IllegalArgumentException if {@code data} is {@code null}.
     */
    public ChunkData process(final ChunkData data) {
        Vldtn.requireNonNull(data, "data");
        ChunkData current = data;
        for (final ChunkFilter filter : filters) {
            current = filter.apply(current);
        }
        return current;
    }

}
