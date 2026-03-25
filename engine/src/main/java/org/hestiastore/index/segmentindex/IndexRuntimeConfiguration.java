package org.hestiastore.index.segmentindex;

import java.util.List;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.chunkstore.ChunkFilterRegistration;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSuppliers;

/**
 * Runtime view of an {@link IndexConfiguration}.
 *
 * <p>
 * {@link IndexConfiguration} stores only persisted metadata that can be written
 * to and loaded from index manifest files. This runtime configuration resolves
 * the persisted chunk filter specs against a {@link ChunkFilterProviderRegistry}
 * and keeps the suppliers needed to materialize fresh runtime filter instances.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexRuntimeConfiguration<K, V> {

    private final IndexConfiguration<K, V> configuration;
    private final List<ChunkFilterRegistration> encodingChunkFilters;
    private final List<ChunkFilterRegistration> decodingChunkFilters;

    private IndexRuntimeConfiguration(final IndexConfiguration<K, V> configuration,
            final List<ChunkFilterRegistration> encodingChunkFilters,
            final List<ChunkFilterRegistration> decodingChunkFilters) {
        this.configuration = Vldtn.requireNonNull(configuration,
                "configuration");
        this.encodingChunkFilters = List.copyOf(Vldtn
                .requireNonNull(encodingChunkFilters, "encodingChunkFilters"));
        this.decodingChunkFilters = List.copyOf(Vldtn
                .requireNonNull(decodingChunkFilters, "decodingChunkFilters"));
    }

    /**
     * Resolves persisted chunk filter specs into runtime suppliers.
     *
     * @param <K> key type
     * @param <V> value type
     * @param configuration persisted configuration
     * @param chunkFilterProviderRegistry registry used to resolve filter specs
     * @return resolved runtime configuration
     */
    public static <K, V> IndexRuntimeConfiguration<K, V> resolve(
            final IndexConfiguration<K, V> configuration,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        final IndexConfiguration<K, V> requiredConfiguration = Vldtn
                .requireNonNull(configuration, "configuration");
        final ChunkFilterProviderRegistry requiredRegistry = Vldtn
                .requireNonNull(chunkFilterProviderRegistry,
                        "chunkFilterProviderRegistry");
        return new IndexRuntimeConfiguration<>(requiredConfiguration,
                toEncodingRegistrations(requiredConfiguration, requiredRegistry),
                toDecodingRegistrations(requiredConfiguration, requiredRegistry));
    }

    /**
     * Returns the persisted configuration this runtime view was resolved from.
     *
     * @return persisted configuration
     */
    public IndexConfiguration<K, V> getConfiguration() {
        return configuration;
    }

    /**
     * Returns runtime encoding filter registrations.
     *
     * @return immutable encoding filter registrations
     */
    public List<ChunkFilterRegistration> getEncodingChunkFilterRegistrations() {
        return encodingChunkFilters;
    }

    /**
     * Returns runtime decoding filter registrations.
     *
     * @return immutable decoding filter registrations
     */
    public List<ChunkFilterRegistration> getDecodingChunkFilterRegistrations() {
        return decodingChunkFilters;
    }

    /**
     * Returns persisted encoding specs.
     *
     * @return immutable encoding filter specs
     */
    public List<ChunkFilterSpec> getEncodingChunkFilterSpecs() {
        return configuration.getEncodingChunkFilterSpecs();
    }

    /**
     * Returns persisted decoding specs.
     *
     * @return immutable decoding filter specs
     */
    public List<ChunkFilterSpec> getDecodingChunkFilterSpecs() {
        return configuration.getDecodingChunkFilterSpecs();
    }

    /**
     * Returns runtime encoding filter suppliers.
     *
     * @return immutable encoding filter suppliers
     */
    public List<Supplier<? extends ChunkFilter>> getEncodingChunkFilterSuppliers() {
        return encodingChunkFilters.stream()
                .map(ChunkFilterRegistration::getSupplier).toList();
    }

    /**
     * Returns runtime decoding filter suppliers.
     *
     * @return immutable decoding filter suppliers
     */
    public List<Supplier<? extends ChunkFilter>> getDecodingChunkFilterSuppliers() {
        return decodingChunkFilters.stream()
                .map(ChunkFilterRegistration::getSupplier).toList();
    }

    /**
     * Materializes runtime encoding filters.
     *
     * @return immutable encoding filter list
     */
    public List<ChunkFilter> getEncodingChunkFilters() {
        return ChunkFilterSuppliers.materialize(getEncodingChunkFilterSuppliers());
    }

    /**
     * Materializes runtime decoding filters.
     *
     * @return immutable decoding filter list
     */
    public List<ChunkFilter> getDecodingChunkFilters() {
        return ChunkFilterSuppliers.materialize(getDecodingChunkFilterSuppliers());
    }

    private static <K, V> List<ChunkFilterRegistration> toEncodingRegistrations(
            final IndexConfiguration<K, V> configuration,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        return configuration.getEncodingChunkFilterSpecs().stream()
                .map(spec -> ChunkFilterRegistration.of(spec,
                        chunkFilterProviderRegistry.createEncodingSupplier(spec)))
                .toList();
    }

    private static <K, V> List<ChunkFilterRegistration> toDecodingRegistrations(
            final IndexConfiguration<K, V> configuration,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        return configuration.getDecodingChunkFilterSpecs().stream()
                .map(spec -> ChunkFilterRegistration.of(spec,
                        chunkFilterProviderRegistry.createDecodingSupplier(spec)))
                .toList();
    }
}
