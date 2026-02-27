package org.hestiastore.index.sorteddatafile;

import java.util.Comparator;

import org.hestiastore.index.ByteTool;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeEncoder;
import org.hestiastore.index.directory.FileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes keys using differential prefix compression.
 *
 * @param <K> key type
 */
public class DiffKeyWriter<K> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final TypeEncoder<K> convertorToBytes;

    private final Comparator<K> keyComparator;

    private byte[] previousKeyBytes;

    private K previousKey;

    /**
     * Creates a diff-key writer with the provided converter and comparator.
     *
     * @param convertorToBytes converter from keys to bytes
     * @param keyComparator comparator used to enforce sorted keys
     */
    public DiffKeyWriter(final TypeEncoder<K> convertorToBytes,
            final Comparator<K> keyComparator) {
        this.convertorToBytes = Vldtn.requireNonNull(convertorToBytes,
                "convertorToBytes");
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        previousKeyBytes = new byte[0];
        previousKey = null;
        logger.trace(
                "Initilizing with conventor to bytes '{}' and comparator '{}'",
                this.convertorToBytes.getClass().getSimpleName(),
                this.keyComparator.getClass().getSimpleName());
    }

    /**
     * Encodes and writes the given key directly into the provided writer as
     * {@code [shared-prefix-len][diff-len][diff-bytes]}.
     *
     * @param writer target writer
     * @param key required key
     * @return number of written bytes
     */
    public int writeTo(final FileWriter writer, final K key) {
        final FileWriter validatedWriter = Vldtn.requireNonNull(writer,
                "writer");
        final EncodedDiffKey diff = encodeDiffKey(key);
        validatedWriter.write((byte) diff.sharedByteLength);
        validatedWriter.write((byte) diff.diffByteLength);
        validatedWriter.write(diff.keyBytes, diff.sharedByteLength,
                diff.diffByteLength);
        return 2 + diff.diffByteLength;
    }

    private byte[] encodeKey(final K key) {
        final int encodedKeyLength = Vldtn.requireGreaterThanOrEqualToZero(
                convertorToBytes.bytesLength(key), "encodedKeyLength");
        final byte[] keyBytes = new byte[encodedKeyLength];
        final int writtenBytes = convertorToBytes.toBytes(key, keyBytes);
        if (writtenBytes != encodedKeyLength) {
            throw new IllegalStateException(String.format(
                    "Encoder wrote '%s' bytes but declared '%s'", writtenBytes,
                    encodedKeyLength));
        }
        return keyBytes;
    }

    private EncodedDiffKey encodeDiffKey(final K key) {
        Vldtn.requireNonNull(key, "key");
        final byte[] keyBytes = encodeKey(key);
        validateKeyOrder(key, keyBytes);
        final int sharedByteLength = ByteTool
                .countMatchingPrefixBytes(previousKeyBytes, keyBytes);
        final int diffByteLength = keyBytes.length - sharedByteLength;
        previousKeyBytes = keyBytes;
        previousKey = key;
        return new EncodedDiffKey(keyBytes, sharedByteLength, diffByteLength);
    }

    private void validateKeyOrder(final K key, final byte[] keyBytes) {
        if (previousKey == null) {
            return;
        }
        final int cmp = keyComparator.compare(previousKey, key);
        if (cmp == 0) {
            final String keyAsString = new String(keyBytes);
            final String keyComparatorClassName = keyComparator.getClass()
                    .getSimpleName();
            throw new IllegalArgumentException(String.format(
                    "Attempt to insers same key as previous. Key '%s' was compared with '%s'",
                    keyAsString, keyComparatorClassName));
        }
        if (cmp > 0) {
            final String previousKeyAsString = new String(previousKeyBytes);
            final String keyAsString = new String(keyBytes);
            final String keyComparatorClassName = keyComparator.getClass()
                    .getSimpleName();
            throw new IllegalArgumentException(String.format(
                    "Attempt to insers key in invalid order. "
                            + "Previous key is '%s', inserted key is '%s' and comparator is '%s'",
                    previousKeyAsString, keyAsString, keyComparatorClassName));
        }
    }

    private static final class EncodedDiffKey {
        private final byte[] keyBytes;
        private final int sharedByteLength;
        private final int diffByteLength;

        private EncodedDiffKey(final byte[] keyBytes,
                final int sharedByteLength, final int diffByteLength) {
            this.keyBytes = keyBytes;
            this.sharedByteLength = sharedByteLength;
            this.diffByteLength = diffByteLength;
        }
    }

    /**
     * Closes the writer. This implementation has no resources to release.
     *
     * @return always 0
     */
    public long close() {
        return 0;
    }
}
