package org.hestiastore.index.sorteddatafile;

import java.util.Comparator;

import org.hestiastore.index.ByteTool;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiffKeyWriter<K> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ConvertorToBytes<K> convertorToBytes;

    private final Comparator<K> keyComparator;

    private byte[] previousKeyBytes;

    private K previousKey;

    private final ByteTool byteTool;

    public DiffKeyWriter(final ConvertorToBytes<K> convertorToBytes,
            final Comparator<K> keyComparator) {
        this.convertorToBytes = Vldtn.requireNonNull(convertorToBytes,
                "convertorToBytes");
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        byteTool = new ByteTool();
        previousKeyBytes = new byte[0];
        previousKey = null;
        logger.trace(
                "Initilizing with conventor to bytes '{}' and comparator '{}'",
                this.convertorToBytes.getClass().getSimpleName(),
                this.keyComparator.getClass().getSimpleName());
    }

    public byte[] write(final K key) {
        Vldtn.requireNonNull(key, "key");
        if (previousKey != null) {
            final int cmp = keyComparator.compare(previousKey, key);
            if (cmp == 0) {
                final String s2 = new String(convertorToBytes.toBytes(key));
                final String keyComapratorClassName = keyComparator.getClass()
                        .getSimpleName();
                throw new IllegalArgumentException(String.format(
                        "Attempt to insers same key as previous. Key '%s' was compared with '%s'",
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
        final byte[] keyBytes = convertorToBytes.toBytes(key);
        final int sharedByteLength = byteTool
                .howMuchBytesIsSame(previousKeyBytes, keyBytes);
        final byte[] diffBytes = byteTool
                .getRemainingBytesAfterIndex(sharedByteLength, keyBytes);

        final byte[] out = new byte[2 + diffBytes.length];
        out[0] = (byte) (sharedByteLength);
        out[1] = (byte) (diffBytes.length);
        System.arraycopy(diffBytes, 0, out, 2, diffBytes.length);

        previousKeyBytes = keyBytes;
        previousKey = key;
        return out;
    }

    public long close() {
        return 0;
    }
}
