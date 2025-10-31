package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.ByteTool;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.MutableBytes;
import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.directory.FileReader;

//FIXME add javadocs
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
            final MutableBytes keyBuffer = MutableBytes
                    .allocate(keyLengthInBytes);
            readFully(fileReader, keyBuffer);
            final Bytes keyBytes = keyBuffer.toBytes();
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
            final String s1 = new String(previousKey.toByteArray());
            throw new IndexException(String.format(
                    "Previous key is '%s' with length '%s'. "
                            + "Current key should share '%s' with previous key.",
                    s1, previousKey.length(), sharedByteLength));
        }
        final MutableBytes diffBuffer = MutableBytes.allocate(keyLengthInBytes);
        readFully(fileReader, diffBuffer);
        final ByteSequence diffBytes = diffBuffer.toBytes();
        final Bytes sharedBytes = previousKey.subBytes(0, sharedByteLength);
        final ByteSequence combined = ByteTool.concatenate(sharedBytes,
                diffBytes);
        final Bytes keyBytes = combined instanceof Bytes ? (Bytes) combined
                : Bytes.copyOf(combined);
        previousKey = keyBytes;
        return keyConvertor.fromBytes(keyBytes);
    }

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
