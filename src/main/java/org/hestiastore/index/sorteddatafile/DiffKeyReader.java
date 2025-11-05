package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.ByteTool;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.directory.FileReader;

public class DiffKeyReader<K> implements TypeReader<K> {

    private final ConvertorFromBytes<K> keyConvertor;

    private byte[] previousKeyBytes;

    private final byte[] header = new byte[2];

    public DiffKeyReader(final ConvertorFromBytes<K> keyConvertor) {
        this.keyConvertor = keyConvertor;
        previousKeyBytes = null;
    }

    @Override
    public K read(final FileReader fileReader) {
        if (2 != fileReader.read(header)) {
            return null;
        }
        final int sharedByteLength = header[0];
        final int keyLengthInBytes = header[1];
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

    private void read(final FileReader fileReader, final byte[] bytes) {
        int read = fileReader.read(bytes);
        if (read != bytes.length) {
            throw new IndexException(String.format(
                    "Reading of '%s' bytes failed just '%s' was read.",
                    bytes.length, read));
        }
    }

    private byte[] getBytes(final byte[] bytes, final int howMany) {
        final byte[] out = new byte[howMany];
        System.arraycopy(bytes, 0, out, 0, howMany);
        return out;
    }

}