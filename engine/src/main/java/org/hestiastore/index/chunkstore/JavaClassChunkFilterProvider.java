package org.hestiastore.index.chunkstore;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;

/**
 * Provider that reconstructs chunk filters by reflective no-args
 * instantiation.
 *
 * <p>
 * This provider exists mainly for compatibility with legacy class-name based
 * metadata and for simple custom filters that intentionally expose a public
 * no-arg constructor.
 * </p>
 */
final class JavaClassChunkFilterProvider implements ChunkFilterProvider {

    @Override
    public String getProviderId() {
        return ChunkFilterProviderRegistry.PROVIDER_ID_JAVA_CLASS;
    }

    @Override
    public Supplier<? extends ChunkFilter> createEncodingSupplier(
            final ChunkFilterSpec spec) {
        return createReflectionSupplier(spec);
    }

    @Override
    public Supplier<? extends ChunkFilter> createDecodingSupplier(
            final ChunkFilterSpec spec) {
        return createReflectionSupplier(spec);
    }

    private Supplier<? extends ChunkFilter> createReflectionSupplier(
            final ChunkFilterSpec spec) {
        requireMatchingProvider(spec);
        final String className = spec.getRequiredParameter(
                ChunkFilterProviderRegistry.PARAM_CLASS_NAME);
        return () -> instantiateFilter(className);
    }

    private void requireMatchingProvider(final ChunkFilterSpec spec) {
        final ChunkFilterSpec requiredSpec = Vldtn.requireNonNull(spec, "spec");
        final String providerId = getProviderId();
        if (!providerId.equals(requiredSpec.getProviderId())) {
            throw new IllegalArgumentException(String.format(
                    "Chunk filter spec provider '%s' does not match '%s'",
                    requiredSpec.getProviderId(), providerId));
        }
    }

    private ChunkFilter instantiateFilter(final String className) {
        final String requiredClassName = Vldtn.requireNotBlank(className,
                ChunkFilterProviderRegistry.PARAM_CLASS_NAME);
        try {
            final Class<?> rawClass = Class.forName(requiredClassName);
            if (!ChunkFilter.class.isAssignableFrom(rawClass)) {
                throw new IllegalArgumentException(String.format(
                        "Class '%s' does not implement ChunkFilter",
                        requiredClassName));
            }
            @SuppressWarnings("unchecked")
            final Class<? extends ChunkFilter> filterClass = (Class<? extends ChunkFilter>) rawClass;
            return filterClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException ex) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unable to instantiate chunk filter class '%s'",
                            requiredClassName),
                    ex);
        }
    }
}
