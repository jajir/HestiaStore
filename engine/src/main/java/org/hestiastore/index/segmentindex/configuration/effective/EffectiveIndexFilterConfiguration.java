package org.hestiastore.index.segmentindex.configuration.effective;

import java.util.List;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.chunkstore.ChunkFilterRegistration;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSuppliers;

/**
 * Resolved runtime chunk filter pipeline configuration.
 */
public final class EffectiveIndexFilterConfiguration {

    private final List<ChunkFilterRegistration> encodingChunkFilters;
    private final List<ChunkFilterRegistration> decodingChunkFilters;

    public EffectiveIndexFilterConfiguration(
            final List<ChunkFilterRegistration> encodingChunkFilters,
            final List<ChunkFilterRegistration> decodingChunkFilters) {
        this.encodingChunkFilters = List.copyOf(Vldtn.requireNonNull(
                encodingChunkFilters, "encodingChunkFilters"));
        this.decodingChunkFilters = List.copyOf(Vldtn.requireNonNull(
                decodingChunkFilters, "decodingChunkFilters"));
    }

    public static EffectiveIndexFilterConfiguration fromSpecs(
            final List<ChunkFilterSpec> encodingChunkFilters,
            final List<ChunkFilterSpec> decodingChunkFilters,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        final ChunkFilterProviderResolver resolver = Vldtn.requireNonNull(
                chunkFilterProviderResolver, "chunkFilterProviderResolver");
        return new EffectiveIndexFilterConfiguration(
                toEncodingRegistrations(encodingChunkFilters, resolver),
                toDecodingRegistrations(decodingChunkFilters, resolver));
    }

    public List<ChunkFilterRegistration> encodingChunkFilterRegistrations() {
        return encodingChunkFilters;
    }

    public List<ChunkFilterRegistration> decodingChunkFilterRegistrations() {
        return decodingChunkFilters;
    }

    public List<ChunkFilterSpec> encodingChunkFilterSpecs() {
        return encodingChunkFilters.stream()
                .map(ChunkFilterRegistration::getSpec).toList();
    }

    public List<ChunkFilterSpec> decodingChunkFilterSpecs() {
        return decodingChunkFilters.stream()
                .map(ChunkFilterRegistration::getSpec).toList();
    }

    public List<Supplier<? extends ChunkFilter>> encodingChunkFilterSuppliers() {
        return ChunkFilterSuppliers.copySuppliers(
                encodingChunkFilters.stream()
                        .map(ChunkFilterRegistration::getSupplier).toList());
    }

    public List<Supplier<? extends ChunkFilter>> decodingChunkFilterSuppliers() {
        return ChunkFilterSuppliers.copySuppliers(
                decodingChunkFilters.stream()
                        .map(ChunkFilterRegistration::getSupplier).toList());
    }

    public List<ChunkFilter> encodingChunkFilters() {
        return ChunkFilterSuppliers.materialize(
                encodingChunkFilterSuppliers());
    }

    public List<ChunkFilter> decodingChunkFilters() {
        return ChunkFilterSuppliers.materialize(
                decodingChunkFilterSuppliers());
    }

    private static List<ChunkFilterRegistration> toEncodingRegistrations(
            final List<ChunkFilterSpec> specs,
            final ChunkFilterProviderResolver resolver) {
        return Vldtn.requireNonNull(specs, "encodingChunkFilters").stream()
                .map(spec -> ChunkFilterRegistration.of(spec,
                        resolver.createEncodingSupplier(spec)))
                .toList();
    }

    private static List<ChunkFilterRegistration> toDecodingRegistrations(
            final List<ChunkFilterSpec> specs,
            final ChunkFilterProviderResolver resolver) {
        return Vldtn.requireNonNull(specs, "decodingChunkFilters").stream()
                .map(spec -> ChunkFilterRegistration.of(spec,
                        resolver.createDecodingSupplier(spec)))
                .toList();
    }
}
