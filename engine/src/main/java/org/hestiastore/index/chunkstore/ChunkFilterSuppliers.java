package org.hestiastore.index.chunkstore;

import java.util.List;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;

/**
 * Utility methods for adapting chunk filters to supplier-based runtime wiring.
 */
public final class ChunkFilterSuppliers {

    private ChunkFilterSuppliers() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Wraps fixed filter instances into singleton suppliers.
     *
     * @param filters required filters
     * @return immutable list of suppliers
     */
    public static List<Supplier<? extends ChunkFilter>> fromFilters(
            final List<ChunkFilter> filters) {
        return Vldtn.requireNonNull(filters, "filters").stream()
                .<Supplier<? extends ChunkFilter>>map(filter -> () -> filter)
                .toList();
    }

    /**
     * Copies suppliers into an immutable list with wildcard-friendly input.
     *
     * @param suppliers required suppliers
     * @return immutable list of suppliers
     */
    public static List<Supplier<? extends ChunkFilter>> copySuppliers(
            final List<? extends Supplier<? extends ChunkFilter>> suppliers) {
        return List.copyOf(Vldtn.requireNonNull(suppliers, "suppliers"));
    }

    /**
     * Creates a fresh filter list from the provided suppliers.
     *
     * @param suppliers required suppliers
     * @return immutable list of materialized filters
     */
    public static List<ChunkFilter> materialize(
            final List<? extends Supplier<? extends ChunkFilter>> suppliers) {
        return copySuppliers(suppliers).stream()
                .map(supplier -> (ChunkFilter) supplier.get()).toList();
    }
}
