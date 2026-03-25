package org.hestiastore.index.chunkstore;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;

/**
 * Provider that always returns one fixed encoding supplier and one fixed
 * decoding supplier for the same provider id.
 *
 * <p>
 * This is used for built-in filter families where the persisted descriptor is
 * just the provider id and no extra parameters are required.
 * </p>
 */
final class FixedPairChunkFilterProvider implements ChunkFilterProvider {

    private final String providerId;
    private final Supplier<? extends ChunkFilter> encodingSupplier;
    private final Supplier<? extends ChunkFilter> decodingSupplier;

    /**
     * Creates a provider backed by fixed encoding and decoding suppliers.
     *
     * @param providerId stable provider id
     * @param encodingSupplier supplier used for write-path filters
     * @param decodingSupplier supplier used for read-path filters
     */
    FixedPairChunkFilterProvider(final String providerId,
            final Supplier<? extends ChunkFilter> encodingSupplier,
            final Supplier<? extends ChunkFilter> decodingSupplier) {
        this.providerId = Vldtn.requireNotBlank(providerId, "providerId");
        this.encodingSupplier = Vldtn.requireNonNull(encodingSupplier,
                "encodingSupplier");
        this.decodingSupplier = Vldtn.requireNonNull(decodingSupplier,
                "decodingSupplier");
    }

    @Override
    public String getProviderId() {
        return providerId;
    }

    @Override
    public Supplier<? extends ChunkFilter> createEncodingSupplier(
            final ChunkFilterSpec spec) {
        requireMatchingProvider(spec);
        return encodingSupplier;
    }

    @Override
    public Supplier<? extends ChunkFilter> createDecodingSupplier(
            final ChunkFilterSpec spec) {
        requireMatchingProvider(spec);
        return decodingSupplier;
    }

    private void requireMatchingProvider(final ChunkFilterSpec spec) {
        final ChunkFilterSpec requiredSpec = Vldtn.requireNonNull(spec, "spec");
        if (!providerId.equals(requiredSpec.getProviderId())) {
            throw new IllegalArgumentException(String.format(
                    "Chunk filter spec provider '%s' does not match '%s'",
                    requiredSpec.getProviderId(), providerId));
        }
    }
}
