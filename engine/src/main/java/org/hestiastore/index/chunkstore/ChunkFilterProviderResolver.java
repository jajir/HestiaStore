package org.hestiastore.index.chunkstore;

import java.util.function.Supplier;

/**
 * Resolves persisted chunk filter metadata into runtime filter suppliers.
 */
public interface ChunkFilterProviderResolver {

    String PROVIDER_ID_CRC32 = "crc32";
    String PROVIDER_ID_MAGIC_NUMBER = "magic-number";
    String PROVIDER_ID_SNAPPY = "snappy";
    String PROVIDER_ID_XOR = "xor";
    String PROVIDER_ID_DO_NOTHING = "do-nothing";
    String PROVIDER_ID_JAVA_CLASS = "java-class";
    String PARAM_CLASS_NAME = "className";

    /**
     * Resolves an encoding supplier for a persisted filter spec.
     *
     * @param spec persisted filter spec
     * @return encoding supplier used to create runtime write-path filters
     */
    Supplier<? extends ChunkFilter> createEncodingSupplier(
            ChunkFilterSpec spec);

    /**
     * Resolves a decoding supplier for a persisted filter spec.
     *
     * @param spec persisted filter spec
     * @return decoding supplier used to create runtime read-path filters
     */
    Supplier<? extends ChunkFilter> createDecodingSupplier(
            ChunkFilterSpec spec);
}
