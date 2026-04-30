package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

class ChunkFilterProviderResolverImplTest {

    @Test
    void defaultResolverResolvesBuiltInEncodingAndDecodingSuppliers() {
        final ChunkFilterProviderResolver resolver = ChunkFilterProviderResolverImpl
                .defaultResolver();
        final ChunkFilterSpec magicNumberSpec = ChunkFilterSpecs.magicNumber();

        assertInstanceOf(ChunkFilterMagicNumberWriting.class,
                resolver.createEncodingSupplier(magicNumberSpec).get());
        assertInstanceOf(ChunkFilterMagicNumberValidation.class,
                resolver.createDecodingSupplier(magicNumberSpec).get());
    }

    @Test
    void withProviderReturnsExtendedCopyWithoutMutatingOriginal() {
        final ChunkFilterProviderResolverImpl baseResolver = ChunkFilterProviderResolverImpl
                .builder().build();
        final TrackingProvider provider = new TrackingProvider("custom");

        final ChunkFilterProviderResolverImpl extendedResolver = baseResolver
                .withProvider(provider);

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> baseResolver.getRequired("custom"));
        assertEquals("Unknown chunk filter provider 'custom'",
                exception.getMessage());
        assertSame(provider, extendedResolver.getRequired("custom"));
    }

    @Test
    void builderRejectsDuplicateProviderId() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> ChunkFilterProviderResolverImpl.builder()
                        .withProvider(new TrackingProvider("duplicate"))
                        .withProvider(new TrackingProvider("duplicate")));

        assertEquals("Chunk filter provider 'duplicate' is already registered",
                exception.getMessage());
    }

    @Test
    void createEncodingAndDecodingSuppliersDelegateToRegisteredProvider() {
        final TrackingProvider provider = new TrackingProvider("tracking");
        final ChunkFilterProviderResolver resolver = ChunkFilterProviderResolverImpl
                .builder().withProvider(provider).build();
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider("tracking")
                .withParameter("keyRef", "orders-main");

        final TrackingChunkFilter encodingFilter = (TrackingChunkFilter) resolver
                .createEncodingSupplier(spec).get();
        final TrackingChunkFilter decodingFilter = (TrackingChunkFilter) resolver
                .createDecodingSupplier(spec).get();

        assertEquals(1, provider.getEncodingCalls());
        assertEquals(1, provider.getDecodingCalls());
        assertSame(spec, provider.getLastEncodingSpec());
        assertSame(spec, provider.getLastDecodingSpec());
        assertEquals("encoding-1", encodingFilter.getName());
        assertEquals("decoding-1", decodingFilter.getName());
    }

    private static final class TrackingProvider implements ChunkFilterProvider {

        private final String providerId;
        private final AtomicInteger encodingCalls = new AtomicInteger();
        private final AtomicInteger decodingCalls = new AtomicInteger();
        private ChunkFilterSpec lastEncodingSpec;
        private ChunkFilterSpec lastDecodingSpec;

        private TrackingProvider(final String providerId) {
            this.providerId = providerId;
        }

        @Override
        public String getProviderId() {
            return providerId;
        }

        @Override
        public Supplier<? extends ChunkFilter> createEncodingSupplier(
                final ChunkFilterSpec spec) {
            lastEncodingSpec = spec;
            final int sequence = encodingCalls.incrementAndGet();
            return () -> new TrackingChunkFilter("encoding-" + sequence);
        }

        @Override
        public Supplier<? extends ChunkFilter> createDecodingSupplier(
                final ChunkFilterSpec spec) {
            lastDecodingSpec = spec;
            final int sequence = decodingCalls.incrementAndGet();
            return () -> new TrackingChunkFilter("decoding-" + sequence);
        }

        private int getEncodingCalls() {
            return encodingCalls.get();
        }

        private int getDecodingCalls() {
            return decodingCalls.get();
        }

        private ChunkFilterSpec getLastEncodingSpec() {
            return lastEncodingSpec;
        }

        private ChunkFilterSpec getLastDecodingSpec() {
            return lastDecodingSpec;
        }
    }

    private static final class TrackingChunkFilter implements ChunkFilter {

        private final String name;

        private TrackingChunkFilter(final String name) {
            this.name = name;
        }

        private String getName() {
            return name;
        }

        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }
}
