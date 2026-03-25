package org.hestiastore.index.chunkstore;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;

/**
 * Immutable registry of runtime chunk filter providers.
 *
 * <p>
 * The registry is the bridge between persisted {@link ChunkFilterSpec}
 * metadata and runtime {@link ChunkFilter} instances. Metadata stored in index
 * configuration identifies a provider by {@code providerId}; this registry
 * resolves that provider and asks it to create an encoding or decoding
 * supplier.
 * </p>
 */
public final class ChunkFilterProviderRegistry {

    public static final String PROVIDER_ID_CRC32 = "crc32";
    public static final String PROVIDER_ID_MAGIC_NUMBER = "magic-number";
    public static final String PROVIDER_ID_SNAPPY = "snappy";
    public static final String PROVIDER_ID_XOR = "xor";
    public static final String PROVIDER_ID_DO_NOTHING = "do-nothing";
    public static final String PROVIDER_ID_JAVA_CLASS = "java-class";
    public static final String PARAM_CLASS_NAME = "className";

    private static final ChunkFilterProviderRegistry DEFAULT_REGISTRY = ChunkFilterProviderRegistry
            .builder().withDefaultProviders().build();

    private final Map<String, ChunkFilterProvider> providers;

    private ChunkFilterProviderRegistry(
            final Map<String, ChunkFilterProvider> providers) {
        this.providers = Map.copyOf(Vldtn.requireNonNull(providers,
                "providers"));
    }

    /**
     * Returns immutable registry containing built-in filter providers.
     *
     * @return default built-in registry
     */
    public static ChunkFilterProviderRegistry defaultRegistry() {
        return DEFAULT_REGISTRY;
    }

    /**
     * Creates a builder for immutable registries.
     *
     * @return new registry builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a copy of this registry extended with a single provider.
     *
     * @param provider provider to add
     * @return new registry with the provider added
     */
    public ChunkFilterProviderRegistry withProvider(
            final ChunkFilterProvider provider) {
        final LinkedHashMap<String, ChunkFilterProvider> copy = new LinkedHashMap<>(
                providers);
        putProvider(copy, provider);
        return new ChunkFilterProviderRegistry(copy);
    }

    /**
     * Returns required provider by id.
     *
     * @param providerId stable provider id
     * @return provider instance
     * @throws IllegalArgumentException when no provider is registered for the
     *                                  given id
     */
    public ChunkFilterProvider getRequired(final String providerId) {
        final String requiredProviderId = Vldtn.requireNotBlank(providerId,
                "providerId");
        final ChunkFilterProvider provider = providers.get(requiredProviderId);
        if (provider == null) {
            throw new IllegalArgumentException(String.format(
                    "Unknown chunk filter provider '%s'", requiredProviderId));
        }
        return provider;
    }

    /**
     * Resolves encoding supplier for a spec.
     *
     * @param spec persisted filter spec
     * @return encoding supplier used to create runtime write-path filters
     */
    public Supplier<? extends ChunkFilter> createEncodingSupplier(
            final ChunkFilterSpec spec) {
        final ChunkFilterSpec requiredSpec = Vldtn.requireNonNull(spec, "spec");
        return getRequired(requiredSpec.getProviderId())
                .createEncodingSupplier(requiredSpec);
    }

    /**
     * Resolves decoding supplier for a spec.
     *
     * @param spec persisted filter spec
     * @return decoding supplier used to create runtime read-path filters
     */
    public Supplier<? extends ChunkFilter> createDecodingSupplier(
            final ChunkFilterSpec spec) {
        final ChunkFilterSpec requiredSpec = Vldtn.requireNonNull(spec, "spec");
        return getRequired(requiredSpec.getProviderId())
                .createDecodingSupplier(requiredSpec);
    }

    /**
     * Builder used to assemble immutable provider registries.
     */
    public static final class Builder {

        private final LinkedHashMap<String, ChunkFilterProvider> providers = new LinkedHashMap<>();

        /**
         * Adds the built-in providers shipped with HestiaStore.
         *
         * @return this builder
         */
        public Builder withDefaultProviders() {
            putProvider(providers, new FixedPairChunkFilterProvider(
                    PROVIDER_ID_CRC32,
                    ChunkFilterCrc32Writing::new,
                    ChunkFilterCrc32Validation::new));
            putProvider(providers,
                    new FixedPairChunkFilterProvider(
                            PROVIDER_ID_MAGIC_NUMBER,
                            ChunkFilterMagicNumberWriting::new,
                            ChunkFilterMagicNumberValidation::new));
            putProvider(providers, new FixedPairChunkFilterProvider(
                    PROVIDER_ID_SNAPPY,
                    ChunkFilterSnappyCompress::new,
                    ChunkFilterSnappyDecompress::new));
            putProvider(providers, new FixedPairChunkFilterProvider(
                    PROVIDER_ID_XOR,
                    ChunkFilterXorEncrypt::new, ChunkFilterXorDecrypt::new));
            putProvider(providers, new FixedPairChunkFilterProvider(
                    PROVIDER_ID_DO_NOTHING,
                    ChunkFilterDoNothing::new, ChunkFilterDoNothing::new));
            putProvider(providers, new JavaClassChunkFilterProvider());
            return this;
        }

        /**
         * Adds a custom provider.
         *
         * @param provider provider to add
         * @return this builder
         * @throws IllegalArgumentException when another provider with the same
         *                                  id is already registered
         */
        public Builder withProvider(final ChunkFilterProvider provider) {
            putProvider(providers, provider);
            return this;
        }

        /**
         * Builds immutable registry.
         *
         * @return immutable registry
         */
        public ChunkFilterProviderRegistry build() {
            return new ChunkFilterProviderRegistry(providers);
        }
    }

    private static void putProvider(
            final Map<String, ChunkFilterProvider> providers,
            final ChunkFilterProvider provider) {
        final ChunkFilterProvider requiredProvider = Vldtn
                .requireNonNull(provider, "provider");
        final String providerId = Vldtn
                .requireNotBlank(requiredProvider.getProviderId(),
                        "provider.providerId");
        final ChunkFilterProvider previous = providers.putIfAbsent(providerId,
                requiredProvider);
        if (previous != null && !Objects.equals(previous, requiredProvider)) {
            throw new IllegalArgumentException(String.format(
                    "Chunk filter provider '%s' is already registered",
                    providerId));
        }
    }
}
