package org.hestiastore.index.chunkstore;

import java.util.List;

import org.hestiastore.index.Vldtn;

public class ChunkProcessor {

    private final List<ChunkFilter> filters;

    public ChunkProcessor(final List<ChunkFilter> filters) {
        this.filters = Vldtn.requireNonNull(filters, "filters");
    }

    public void process(final ChunkData data) {
        Vldtn.requireNonNull(data, "data");
        ChunkData current = data;
        for (final ChunkFilter filter : filters) {
            current = filter.apply(current);
        }
    }

}
