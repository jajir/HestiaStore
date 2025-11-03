package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteTool;
import org.hestiastore.index.bytes.ByteSequenceView;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.MutableBytes;
import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.directory.FileReader;

/**
 * Reads keys that were written using {@link DiffKeyWriter} diff encoding. Each
 * call rehydrates the next key by combining the shared prefix stored in the
 * previous key with the key specific suffix stored in the diff stream.
 *
 * @param <K> logical key type produced by the reader
 */
public class DiffKeyReader<K> implements TypeReader<K> {

    private final ConvertorFromBytes<K> keyConvertor;

    private ByteSequence previousKey;

    /**
     * Creates a reader that reconstructs keys using the supplied converter.
     *
     * @param keyConvertor converts raw {@link ByteSequenceView} into the
     *                     logical key type
     */
    public DiffKeyReader(final ConvertorFromBytes<K> keyConvertor) {
        this.keyConvertor = keyConvertor;
        previousKey = null;
    }

    /**
     * Reads the next key from the provided {@link FileReader}. The stream is
     * expected to be encoded as: shared prefix length byte, diff length byte,
     * followed by the diff payload. Returns {@code null} when no more keys are
     * available.
     */
    @Override
    public K read(final FileReader fileReader) {
        final int sharedByteLength = fileReader.read();
        if (sharedByteLength == -1) {
            return null;
        }
        final int keyLengthInBytes = fileReader.read();
        if (sharedByteLength == 0) {
            final MutableBytes keyBuffer = MutableBytes
                    .allocate(keyLengthInBytes);
            readFully(fileReader, keyBuffer);
            previousKey = keyBuffer;
            return keyConvertor.fromBytes(previousKey);
        }
        if (previousKey == null) {
            throw new IndexException(String
                    .format("Unable to read key because there should be '%s' "
                            + "bytes shared with previous key but there is no"
                            + " previous key", sharedByteLength));
        }
        if (previousKey.length() < sharedByteLength) {
            final String s1 = new String(previousKey.toByteArray());
            throw new IndexException(String.format(
                    "Previous key is '%s' with length '%s'. "
                            + "Current key should share '%s' with previous key.",
                    s1, previousKey.length(), sharedByteLength));
        }
        final MutableBytes diffBuffer = MutableBytes.allocate(keyLengthInBytes);
        readFully(fileReader, diffBuffer);
        final ByteSequence diffBytes = diffBuffer;
        final ByteSequence sharedBytes = previousKey.slice(0, sharedByteLength);
        final ByteSequence combined = ByteTool.concatenate(sharedBytes,
                diffBytes);
        previousKey = combined;
        return keyConvertor.fromBytes(combined);
    }

    /**
     * Reads a full buffer from the file or throws if the stream ends
     * prematurely, ensuring diff reconstruction sees complete suffix data.
     */
    private void readFully(final FileReader fileReader,
            final MutableBytes bytes) {
        int read = fileReader.read(bytes);
        if (read != bytes.length()) {
            throw new IndexException(String.format(
                    "Reading of '%s' bytes failed just '%s' was read.",
                    bytes.length(), read));
        }
    }

}
