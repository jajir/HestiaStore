package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ChunkFilterSpecsTest {

    @Test
    void fromPersistedClassNameCanonicalizesBuiltInReadAndWriteClasses() {
        assertEquals(ChunkFilterSpecs.crc32(), ChunkFilterSpecs
                .fromPersistedClassName(ChunkFilterCrc32Writing.class.getName()));
        assertEquals(ChunkFilterSpecs.crc32(), ChunkFilterSpecs.fromPersistedClassName(
                ChunkFilterCrc32Validation.class.getName()));
        assertEquals(ChunkFilterSpecs.magicNumber(),
                ChunkFilterSpecs.fromPersistedClassName(
                        ChunkFilterMagicNumberWriting.class.getName()));
        assertEquals(ChunkFilterSpecs.magicNumber(),
                ChunkFilterSpecs.fromPersistedClassName(
                        ChunkFilterMagicNumberValidation.class.getName()));
        assertEquals(ChunkFilterSpecs.snappy(),
                ChunkFilterSpecs.fromPersistedClassName(
                        ChunkFilterSnappyCompress.class.getName()));
        assertEquals(ChunkFilterSpecs.snappy(),
                ChunkFilterSpecs.fromPersistedClassName(
                        ChunkFilterSnappyDecompress.class.getName()));
    }

    @Test
    void canonicalizeTurnsBuiltInJavaClassSpecsIntoStableProviderSpecs() {
        final ChunkFilterSpec javaClassSpec = ChunkFilterSpecs
                .javaClass(ChunkFilterDoNothing.class);

        assertEquals(ChunkFilterSpecs.doNothing(),
                ChunkFilterSpecs.canonicalize(javaClassSpec));
    }

    @Test
    void canonicalizeLeavesUnknownJavaClassSpecsUntouched() {
        final ChunkFilterSpec javaClassSpec = ChunkFilterSpecs
                .javaClass(CustomChunkFilter.class);

        assertEquals(javaClassSpec, ChunkFilterSpecs.canonicalize(javaClassSpec));
    }

    @Test
    void forEncodingAndDecodingFiltersMapBuiltInsAndCustomClasses() {
        assertEquals(ChunkFilterSpecs.xor(),
                ChunkFilterSpecs.forEncodingFilter(
                        ChunkFilterXorEncrypt.class));
        assertEquals(ChunkFilterSpecs.xor(),
                ChunkFilterSpecs.forDecodingFilter(
                        ChunkFilterXorDecrypt.class));
        assertEquals(ChunkFilterSpecs.javaClass(CustomChunkFilter.class),
                ChunkFilterSpecs.forEncodingFilter(new CustomChunkFilter()));
        assertEquals(ChunkFilterSpecs.javaClass(CustomChunkFilter.class),
                ChunkFilterSpecs.forDecodingFilter(new CustomChunkFilter()));
    }

    @Test
    void javaClassSpecStoresClassNameParameter() {
        final ChunkFilterSpec spec = ChunkFilterSpecs
                .javaClass(CustomChunkFilter.class);

        assertEquals(ChunkFilterProviderRegistry.PROVIDER_ID_JAVA_CLASS,
                spec.getProviderId());
        assertEquals(CustomChunkFilter.class.getName(),
                spec.getRequiredParameter(
                        ChunkFilterProviderRegistry.PARAM_CLASS_NAME));
    }

    private static final class CustomChunkFilter implements ChunkFilter {

        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }
}
