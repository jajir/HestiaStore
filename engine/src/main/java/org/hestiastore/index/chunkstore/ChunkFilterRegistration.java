package org.hestiastore.index.chunkstore;

import java.util.Objects;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;

/**
 * Runtime registration that couples persisted filter metadata with the
 * supplier used to create filter instances.
 */
public final class ChunkFilterRegistration {

    private final ChunkFilterSpec spec;
    private final Supplier<? extends ChunkFilter> supplier;

    private ChunkFilterRegistration(final ChunkFilterSpec spec,
            final Supplier<? extends ChunkFilter> supplier) {
        this.spec = Vldtn.requireNonNull(spec, "spec");
        this.supplier = Vldtn.requireNonNull(supplier, "supplier");
    }

    /**
     * Creates a registration from spec and supplier.
     *
     * @param spec persisted filter descriptor
     * @param supplier runtime supplier creating filter instances
     * @return new registration
     */
    public static ChunkFilterRegistration of(final ChunkFilterSpec spec,
            final Supplier<? extends ChunkFilter> supplier) {
        return new ChunkFilterRegistration(spec, supplier);
    }

    /**
     * Returns persisted filter descriptor.
     *
     * @return filter spec
     */
    public ChunkFilterSpec getSpec() {
        return spec;
    }

    /**
     * Returns runtime filter supplier.
     *
     * <p>
     * The supplier is a runtime concern only. It is not persisted directly; the
     * associated {@link #getSpec()} descriptor is the persisted source of
     * truth.
     * </p>
     *
     * @return runtime supplier
     */
    public Supplier<? extends ChunkFilter> getSupplier() {
        return supplier;
    }

    @Override
    public int hashCode() {
        return Objects.hash(spec, supplier);
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ChunkFilterRegistration other)) {
            return false;
        }
        return Objects.equals(spec, other.spec)
                && Objects.equals(supplier, other.supplier);
    }

    @Override
    public String toString() {
        return spec.toString();
    }
}
