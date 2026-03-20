package org.hestiastore.index.sorteddatafile;

import java.nio.charset.StandardCharsets;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDecoder;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.directory.FileReader;

/**
 * Reader that reconstructs keys encoded with differential prefix compression.
 * <p>
 * Each key is encoded as a 2-byte header followed by a variable-length
 * payload. The header has the following layout:
 * <ul>
 * <li>byte[0]: number of prefix bytes shared with the previously decoded key
 * (0..255)</li>
 * <li>byte[1]: number of following bytes that represent the non-shared suffix
 * (0..255)</li>
 * </ul>
 * For the first key in a stream, the shared-prefix length must be 0 and the
 * payload contains the entire key. For subsequent keys, the reader takes the
 * first {@code shared} bytes from the previously decoded key and concatenates
 * them with the {@code suffix} bytes from the current payload to reconstruct
 * the full key.
 * <p>
 * The reader keeps an internal copy of the previously decoded key and is not
 * thread-safe. Create a new instance when starting a new scan.
 */
public class DiffKeyReader<K> implements TypeReader<K> {

    private final TypeDecoder<K> keyConvertor;

    private byte[] previousKeyBytes;

    private final byte[] header = new byte[2];

    /**
     * Create a reader using the provided converter to turn raw bytes into
     * keys.
     * 
     * @param keyConvertor required converter from bytes to key type
     */
    public DiffKeyReader(final TypeDecoder<K> keyConvertor) {
        this.keyConvertor = keyConvertor;
        previousKeyBytes = null;
    }

    /**
     * Read and decode the next key from the given {@link FileReader}.
     * <p>
     * Behavior:
     * <ul>
     * <li>If fewer than 2 bytes are available for the header, returns
     * {@code null} (end of stream).</li>
     * <li>If the header declares a non-zero shared prefix for the very first
     * key, an {@link IndexException} is thrown.</li>
     * <li>If the declared shared-prefix length exceeds the length of the
     * previously decoded key, an {@link IndexException} is thrown.</li>
     * <li>If the payload bytes cannot be fully read, an {@link IndexException}
     * is thrown.</li>
     * </ul>
     * 
     * @param fileReader required data source
     * @return decoded key or {@code null} when end of input is reached
     * @throws IndexException when the encoded stream is inconsistent or I/O
     *                        does not return the expected number of bytes
     */
    @Override
    public K read(final FileReader fileReader) {
        if (2 != fileReader.read(header)) {
            return null;
        }
        final int sharedByteLength = Byte.toUnsignedInt(header[0]);
        final int keyLengthInBytes = Byte.toUnsignedInt(header[1]);
        if (sharedByteLength == 0) {
            final byte[] keyBytes = new byte[keyLengthInBytes];
            read(fileReader, keyBytes, 0, keyLengthInBytes);
            previousKeyBytes = keyBytes;
            return keyConvertor.decode(keyBytes);
        }
        if (previousKeyBytes == null) {
            throw new IndexException(String
                    .format("Unable to read key because there should be '%s' "
                            + "bytes shared with previous key but there is no"
                            + " previous key", sharedByteLength));
        }
        if (previousKeyBytes.length < sharedByteLength) {
            final String s1 = new String(previousKeyBytes,
                    StandardCharsets.ISO_8859_1);
            throw new IndexException(String.format(
                    "Previous key is '%s' with length '%s'. "
                            + "Current key should share '%s' with previous key.",
                    s1, previousKeyBytes.length, sharedByteLength));
        }
        final int totalKeyLength = sharedByteLength + keyLengthInBytes;
        final byte[] keyBytes = new byte[totalKeyLength];
        System.arraycopy(previousKeyBytes, 0, keyBytes, 0, sharedByteLength);
        read(fileReader, keyBytes, sharedByteLength, keyLengthInBytes);
        previousKeyBytes = keyBytes;
        return keyConvertor.decode(keyBytes);
    }

    /**
     * Reads the requested number of bytes or throws when the stream ends.
     *
     * @param fileReader source reader
     * @param bytes destination buffer
     */
    private void read(final FileReader fileReader, final byte[] bytes,
            final int offset, final int length) {
        if (length == 0) {
            return;
        }
        int currentOffset = offset;
        int remaining = length;
        while (remaining > 0) {
            final int read;
            if (currentOffset == 0 && remaining == bytes.length) {
                read = fileReader.read(bytes);
            } else {
                read = fileReader.read(bytes, currentOffset, remaining);
            }
            if (read <= 0) {
                throw new IndexException(String.format(
                        "Reading of '%s' bytes failed just '%s' was read.",
                        length, length - remaining));
            }
            currentOffset += read;
            remaining -= read;
        }
    }

}
