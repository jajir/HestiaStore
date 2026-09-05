package org.hestiastore.index.senku.internal;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkentryfile.KeyPageCodec;
import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkstore.Compression;

/** Immutable persisted key encoding and chunk compression for one index. */
final class SenkuStorageFormat {
    private final KeyPageCodec<?> keyCodec;
    private final Compression compression;

    SenkuStorageFormat(final KeyPageCodec<?> codec,
            final Compression compression) {
        keyCodec = Vldtn.requireNonNull(codec, "keyPageCodec");
        this.compression = Vldtn.requireNonNull(compression, "compression");
    }

    /** @return generic prefix keys and Zstd level 3 */
    static SenkuStorageFormat createDefault() {
        return new SenkuStorageFormat(KeyPageCodecs.prefix(),
                Compression.zstd(3));
    }

    /**
     * @param <K> expected key type
     * @return codec to validate against its descriptor
     */
    @SuppressWarnings("unchecked")
    <K> KeyPageCodec<K> keyCodec() {
        return (KeyPageCodec<K>) keyCodec;
    }

    /** @return chunk compression setting */
    Compression compression() {
        return compression;
    }
}
