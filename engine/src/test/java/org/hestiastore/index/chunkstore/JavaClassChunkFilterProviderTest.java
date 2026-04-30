package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class JavaClassChunkFilterProviderTest {

    private final JavaClassChunkFilterProvider provider = new JavaClassChunkFilterProvider();

    @Test
    void createsFreshEncodingAndDecodingInstances() {
        final ChunkFilterSpec spec = ChunkFilterSpecs
                .javaClass(ReflectiveChunkFilter.class);

        final ChunkFilter first = provider.createEncodingSupplier(spec).get();
        final ChunkFilter second = provider.createDecodingSupplier(spec).get();

        assertInstanceOf(ReflectiveChunkFilter.class, first);
        assertInstanceOf(ReflectiveChunkFilter.class, second);
        assertNotSame(first, second);
    }

    @Test
    void rejectsMismatchedProviderId() {
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider("custom")
                .withParameter(ChunkFilterProviderResolver.PARAM_CLASS_NAME,
                        ReflectiveChunkFilter.class.getName());

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> provider.createEncodingSupplier(spec));

        assertEquals(
                "Chunk filter spec provider 'custom' does not match 'java-class'",
                exception.getMessage());
    }

    @Test
    void rejectsSpecWithoutClassNameParameter() {
        final ChunkFilterSpec spec = ChunkFilterSpec
                .ofProvider(ChunkFilterProviderResolver.PROVIDER_ID_JAVA_CLASS);

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> provider.createEncodingSupplier(spec));

        assertEquals(
                "Missing required chunk filter parameter 'className' for provider 'java-class'",
                exception.getMessage());
    }

    @Test
    void rejectsClassesThatDoNotImplementChunkFilter() {
        final ChunkFilterSpec spec = ChunkFilterSpecs.javaClass(String.class
                .getName());

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> provider.createEncodingSupplier(spec).get());

        assertEquals("Class 'java.lang.String' does not implement ChunkFilter",
                exception.getMessage());
    }

    @Test
    void rejectsClassesWithoutNoArgsConstructor() {
        final ChunkFilterSpec spec = ChunkFilterSpecs
                .javaClass(NoDefaultConstructorChunkFilter.class);

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> provider.createEncodingSupplier(spec).get());

        assertEquals(String.format(
                "Unable to instantiate chunk filter class '%s'",
                NoDefaultConstructorChunkFilter.class.getName()),
                exception.getMessage());
    }

    @Test
    void rejectsUnknownClasses() {
        final ChunkFilterSpec spec = ChunkFilterSpecs
                .javaClass("missing.ChunkFilter");

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> provider.createDecodingSupplier(spec).get());

        assertEquals(
                "Unable to instantiate chunk filter class 'missing.ChunkFilter'",
                exception.getMessage());
    }

    public static final class ReflectiveChunkFilter implements ChunkFilter {

        public ReflectiveChunkFilter() {
        }

        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }

    public static final class NoDefaultConstructorChunkFilter
            implements ChunkFilter {

        public NoDefaultConstructorChunkFilter(final String ignored) {
        }

        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }
}
