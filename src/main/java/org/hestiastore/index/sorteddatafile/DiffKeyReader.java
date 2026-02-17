package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.ByteTool;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.ConvertorFromBytes;
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

    private final ConvertorFromBytes<K> keyConvertor;

    private byte[] previousKeyBytes;

    private final byte[] header = new byte[2];

    /**
     * Create a reader using the provided converter to turn raw bytes into
     * keys.
     * 
     * @param keyConvertor required converter from bytes to key type
     */
    public DiffKeyReader(final ConvertorFromBytes<K> keyConvertor) {
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
        final int sharedByteLength = header[0];
        final int keyLengthInBytes = header[1]; // number of suffix bytes
        if (sharedByteLength == 0) {
            final byte[] keyBytes = new byte[keyLengthInBytes];
            read(fileReader, keyBytes);
            previousKeyBytes = keyBytes;
            return keyConvertor.fromBytes(keyBytes);
        }
        if (previousKeyBytes == null) {
            throw new IndexException(String
                    .format("Unable to read key because there should be '%s' "
                            + "bytes shared with previous key but there is no"
                            + " previous key", sharedByteLength));
        }
        if (previousKeyBytes.length < sharedByteLength) {
            final String s1 = new String(previousKeyBytes);
            throw new IndexException(String.format(
                    "Previous key is '%s' with length '%s'. "
                            + "Current key should share '%s' with previous key.",
                    s1, previousKeyBytes.length, sharedByteLength));
        }
        final byte[] diffBytes = new byte[keyLengthInBytes];
        read(fileReader, diffBytes);
        final byte[] sharedBytes = getBytes(previousKeyBytes, sharedByteLength);
        final byte[] keyBytes = ByteTool.concatenate(sharedBytes, diffBytes);
        previousKeyBytes = keyBytes;
        return keyConvertor.fromBytes(keyBytes);
    }

    /**
     * Reads the requested number of bytes or throws when the stream ends.
     *
     * @param fileReader source reader
     * @param bytes destination buffer
     */
    private void read(final FileReader fileReader, final byte[] bytes) {
        int read = fileReader.read(bytes);
        if (read != bytes.length) {
            throw new IndexException(String.format(
                    "Reading of '%s' bytes failed just '%s' was read.",
                    bytes.length, read));
        }
    }

    /**
     * Copies the first {@code howMany} bytes into a new array.
     *
     * @param bytes source bytes
     * @param howMany number of bytes to copy
     * @return copied bytes
     */
    private byte[] getBytes(final byte[] bytes, final int howMany) {
        final byte[] out = new byte[howMany];
        System.arraycopy(bytes, 0, out, 0, howMany);
        return out;
    }

}
