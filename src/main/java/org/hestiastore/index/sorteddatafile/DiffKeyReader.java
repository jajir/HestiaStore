package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.directory.FileReader;

public final class DiffKeyReader<K> implements TypeReader<K> {

    private final ConvertorFromBytes<K> keyConvertor;

    /**
     * Previously decoded full key bytes; used to reuse the shared prefix.
     * Empty array means "no previous key".
     */
    private byte[] previousKeyBytes = new byte[0];

    /** Reusable 2-byte header buffer: [sharedPrefixLength, diffLength]. */
    private final byte[] header = new byte[2];

    public DiffKeyReader(final ConvertorFromBytes<K> keyConvertor) {
        this.keyConvertor = keyConvertor;
    }

    @Override
    public K read(final FileReader reader) {
        // Read 2-byte header. If EOF before first byte, signal end by returning null.
        final int first = reader.read();
        if (first == -1) {
            return null;
        }
        header[0] = (byte) first;
        // ensure we get the second byte
        int second = reader.read();
        if (second == -1) {
            throw new IndexException("Incomplete key header: missing diff length byte");
        }
        header[1] = (byte) second;

        final int sharedLen = header[0] & 0xFF;
        final int diffLen = header[1] & 0xFF;

        if (sharedLen == 0) {
            // Fast path: whole key is diff; read directly into final array
            final byte[] keyBytes = new byte[diffLen];
            readFully(reader, keyBytes, 0, diffLen);
            previousKeyBytes = keyBytes;
            return keyConvertor.fromBytes(keyBytes);
        }

        if (previousKeyBytes.length < sharedLen) {
            throw new IndexException(String.format(
                    "Previous key length '%s' smaller than shared prefix '%s'",
                    previousKeyBytes.length, sharedLen));
        }

        // Allocate final key buffer once and fill it: shared prefix + diff
        final byte[] keyBytes = new byte[sharedLen + diffLen];
        System.arraycopy(previousKeyBytes, 0, keyBytes, 0, sharedLen);
        readFully(reader, keyBytes, sharedLen, diffLen);
        previousKeyBytes = keyBytes;
        return keyConvertor.fromBytes(keyBytes);
    }

    private void readFully(final FileReader reader, final byte[] dst,
            final int off, final int len) {
        // Request exactly the remaining bytes in a single read, mirroring
        // writer behavior and test expectations. If fewer bytes are returned,
        // treat as an error.
        final byte[] chunk = new byte[len];
        final int r = reader.read(chunk);
        if (r != len) {
            throw new IndexException(String.format(
                    "Reading of '%s' bytes failed: just '%s' was read.", len,
                    r));
        }
        System.arraycopy(chunk, 0, dst, off, len);
    }
}
