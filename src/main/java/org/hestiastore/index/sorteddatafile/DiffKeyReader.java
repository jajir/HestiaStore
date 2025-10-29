package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.ByteTool;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.directory.FileReader;

public class DiffKeyReader<K> implements TypeReader<K> {

    private final ConvertorFromBytes<K> keyConvertor;

    private Bytes previousKey;

    public DiffKeyReader(final ConvertorFromBytes<K> keyConvertor) {
        this.keyConvertor = keyConvertor;
        previousKey = null;
    }

    @Override
    public K read(final FileReader fileReader) {
        final int sharedByteLength = fileReader.read();
        if (sharedByteLength == -1) {
            return null;
        }
        final int keyLengthInBytes = fileReader.read();
        if (sharedByteLength == 0) {
            final Bytes keyBytes = Bytes.allocate(keyLengthInBytes);
            read(fileReader, keyBytes);
            previousKey = keyBytes;
            return keyConvertor.fromBytes(keyBytes);
        }
        if (previousKey == null) {
            throw new IndexException(String
                    .format("Unable to read key because there should be '%s' "
                            + "bytes shared with previous key but there is no"
                            + " previous key", sharedByteLength));
        }
        if (previousKey.length() < sharedByteLength) {
            final String s1 = new String(previousKey.getData());
            throw new IndexException(String.format(
                    "Previous key is '%s' with length '%s'. "
                            + "Current key should share '%s' with previous key.",
                    s1, previousKey.length(), sharedByteLength));
        }
        final Bytes diffBytes = Bytes.allocate(keyLengthInBytes);
        read(fileReader, diffBytes);
        final Bytes sharedBytes = previousKey.subBytes(0, sharedByteLength);
        final Bytes keyBytes = ByteTool.concatenate(sharedBytes, diffBytes);
        previousKey = keyBytes;
        return keyConvertor.fromBytes(keyBytes);
    }

    private void read(final FileReader fileReader, final Bytes bytes) {
        int read = fileReader.read(bytes);
        if (read != bytes.length()) {
            throw new IndexException(String.format(
                    "Reading of '%s' bytes failed just '%s' was read.",
                    bytes.length(), read));
        }
    }

}
