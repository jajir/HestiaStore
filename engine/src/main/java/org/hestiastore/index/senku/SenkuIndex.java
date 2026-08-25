package org.hestiastore.index.senku;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.senku.internal.SenkuRuntime;

/**
 * Entry point for creating and opening Senku indexes.
 */
public final class SenkuIndex {

    private SenkuIndex() {
        // Static entry point.
    }

    /**
     * Creates a minimal builder for a new index.
     *
     * @param <K>                 key type
     * @param <V>                 value type
     * @param directory           target directory
     * @param keyTypeDescriptor   key descriptor
     * @param valueTypeDescriptor value descriptor
     * @param functions           registry containing the duplicate merge
     *                            function
     * @return new builder
     */
    public static <K, V> SenkuIndexBuilder<K, V> builder(
            final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final SenkuMergeFunctionRegistry<K, V> functions) {
        return new SenkuIndexBuilder<>(
                Vldtn.requireNonNull(directory, "directory"),
                Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor"),
                Vldtn.requireNonNull(valueTypeDescriptor,
                        "valueTypeDescriptor"),
                Vldtn.requireNonNull(functions, "functions"));
    }

    /**
     * Opens an already-finalized Senku index exclusively.
     *
     * @param <K> key type
     * @param <V> value type
     * @param directory ready index directory
     * @param keyTypeDescriptor key descriptor
     * @param valueTypeDescriptor value descriptor
     * @param diskIoBufferSize original chunk-store I/O buffer size
     * @return exclusive ready handle
     */
    public static <K, V> SenkuReady<K, V> open(
            final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int diskIoBufferSize) {
        return SenkuRuntime.open(directory, keyTypeDescriptor,
                valueTypeDescriptor, diskIoBufferSize);
    }
}
