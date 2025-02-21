package com.coroptis.index.sorteddatafile;

import java.util.Comparator;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.ByteTool;
import com.coroptis.index.CloseableResource;
import com.coroptis.index.datatype.ConvertorToBytes;
import com.coroptis.index.directory.FileWriter;

public class DiffKeyWriter<K> implements CloseableResource {

    private final Logger logger = LoggerFactory.getLogger(DiffKeyWriter.class);

    private final ConvertorToBytes<K> convertorToBytes;

    private final Comparator<K> keyComparator;

    private final FileWriter fileWriter;

    private byte[] previousKeyBytes;

    private K previousKey;

    private final ByteTool byteTool;

    public DiffKeyWriter(final ConvertorToBytes<K> convertorToBytes,
            final Comparator<K> keyComparator, final FileWriter fileWriter) {
        this.convertorToBytes = Objects.requireNonNull(convertorToBytes,
                "Convertor to bytes is null");
        this.keyComparator = Objects.requireNonNull(keyComparator,
                "Key comparator can't be null");
        this.fileWriter = Objects.requireNonNull(fileWriter,
                "FileWriter can't be null");
        byteTool = new ByteTool();
        previousKeyBytes = new byte[0];
        previousKey = null;
        logger.trace(
                "Initilizing with conventor to bytes '{}' and comparator '{}' and fileWriter '{}'",
                this.convertorToBytes.getClass().getSimpleName(),
                this.keyComparator.getClass().getSimpleName(),
                this.fileWriter.getClass().getSimpleName());
    }

    public int write(final K key, final boolean fullWrite) {
        Objects.requireNonNull(key, "key can't be null");
        if (previousKey != null) {
            final int cmp = keyComparator.compare(previousKey, key);
            if (cmp == 0) {
                final String s2 = new String(convertorToBytes.toBytes(key));
                final String keyComapratorClassName = keyComparator.getClass()
                        .getSimpleName();
                throw new IllegalArgumentException(String.format(
                        "Attempt to insers same key as previous. Key '%s' was comapred with '%s'",
                        s2, keyComapratorClassName));
            }
            if (cmp > 0) {
                final String s1 = new String(previousKeyBytes);
                final String s2 = new String(convertorToBytes.toBytes(key));
                final String keyComapratorClassName = keyComparator.getClass()
                        .getSimpleName();
                throw new IllegalArgumentException(String.format(
                        "Attempt to insers key in invalid order. "
                                + "Previous key is '%s', inserted key is '%s' and comparator is '%s'",
                        s1, s2, keyComapratorClassName));
            }
        }
        if (fullWrite) {
            final byte[] keyBytes = convertorToBytes.toBytes(key);

            return write(0, keyBytes, key, keyBytes);
        } else {
            final byte[] keyBytes = convertorToBytes.toBytes(key);
            final int sharedByteLength = byteTool
                    .howMuchBytesIsSame(previousKeyBytes, keyBytes);
            final byte[] diffBytes = byteTool
                    .getRemainingBytesAfterIndex(sharedByteLength, keyBytes);

            return write(sharedByteLength, diffBytes, key,
                    keyBytes);
        }
    }

    private int write(final int sharedByteLength,
            final byte[] diffBytes, final K key, final byte[] keyBytes) {
        fileWriter.write((byte) (sharedByteLength));
        fileWriter.write((byte) (diffBytes.length));
        fileWriter.write(diffBytes);

        previousKeyBytes = keyBytes;
        previousKey = key;
        return 2 + diffBytes.length;
    }

    @Override
    public void close() {
            fileWriter.close();
    }

}
