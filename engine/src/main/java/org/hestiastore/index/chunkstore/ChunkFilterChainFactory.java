package org.hestiastore.index.chunkstore;

import java.util.List;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;

/**
 * Immutable factory for materializing chunk filter chains.
 *
 * <p>
 * Long-lived components such as {@link ChunkStoreFile} keep this factory
 * instead of storing concrete {@link ChunkFilter} instances directly. That
 * lets them create a fresh runtime chain whenever a reader or writer is
 * opened, while still supporting the simpler fixed-instance configuration
 * style.
 * </p>
 */
public final class ChunkFilterChainFactory {

    private final List<Supplier<? extends ChunkFilter>> suppliers;

    private ChunkFilterChainFactory(
            final List<? extends Supplier<? extends ChunkFilter>> suppliers) {
        this.suppliers = ChunkFilterSuppliers.copySuppliers(suppliers);
    }

    /**
     * Creates a factory from fixed chunk filter instances.
     *
     * @param filters required filters
     * @return immutable chain factory
     */
    public static ChunkFilterChainFactory fromFilters(
            final List<ChunkFilter> filters) {
        return new ChunkFilterChainFactory(ChunkFilterSuppliers
                .fromFilters(Vldtn.requireNonNull(filters, "filters")));
    }

    /**
     * Creates a factory from runtime filter suppliers.
     *
     * @param suppliers required suppliers
     * @return immutable chain factory
     */
    public static ChunkFilterChainFactory fromSuppliers(
            final List<? extends Supplier<? extends ChunkFilter>> suppliers) {
        return new ChunkFilterChainFactory(
                Vldtn.requireNonNull(suppliers, "suppliers"));
    }

    /**
     * Materializes a fresh filter list for a runtime reader/writer instance.
     *
     * <p>
     * When this factory was created from suppliers, each call may return newly
     * constructed filter instances. When it was created from fixed filter
     * instances, the same underlying objects may appear again.
     * </p>
     *
     * @return immutable filter list ready for runtime use
     */
    public List<ChunkFilter> materialize() {
        return ChunkFilterSuppliers.materialize(suppliers);
    }

    /**
     * Returns immutable suppliers used by this factory.
     *
     * @return immutable supplier list
     */
    public List<Supplier<? extends ChunkFilter>> getSuppliers() {
        return suppliers;
    }
}
