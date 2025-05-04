package com.coroptis.index.sorteddatafile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.ByteTool;
import com.coroptis.index.ContextAwareLogger;
import com.coroptis.index.LoggingContext;
import com.coroptis.index.datatype.ConvertorToBytes;

public class DiffKeyWriter<K> {

        private final ContextAwareLogger logger;

        private final ConvertorToBytes<K> convertorToBytes;

        private final Comparator<K> keyComparator;

        private byte[] previousKeyBytes;

        private K previousKey;

        private final ByteTool byteTool;

        public DiffKeyWriter(final LoggingContext loggingContext,
                        final ConvertorToBytes<K> convertorToBytes,
                        final Comparator<K> keyComparator) {
                this.logger = new ContextAwareLogger(DiffKeyWriter.class,
                                loggingContext);
                this.convertorToBytes = Objects.requireNonNull(convertorToBytes,
                                "Convertor to bytes is null");
                this.keyComparator = Objects.requireNonNull(keyComparator,
                                "Key comparator can't be null");
                byteTool = new ByteTool();
                previousKeyBytes = new byte[0];
                previousKey = null;
                logger.trace("Initilizing with conventor to bytes '{}' and comparator '{}'",
                                this.convertorToBytes.getClass()
                                                .getSimpleName(),
                                this.keyComparator.getClass().getSimpleName());
        }

        public byte[] write(final K key) {
                Objects.requireNonNull(key, "key can't be null");
                if (previousKey != null) {
                        final int cmp = keyComparator.compare(previousKey, key);
                        if (cmp == 0) {
                                final String s2 = new String(
                                                convertorToBytes.toBytes(key));
                                final String keyComapratorClassName = keyComparator
                                                .getClass().getSimpleName();
                                throw new IllegalArgumentException(String
                                                .format("Attempt to insers same key as previous. Key '%s' was comapred with '%s'",
                                                                s2,
                                                                keyComapratorClassName));
                        }
                        if (cmp > 0) {
                                final String s1 = new String(previousKeyBytes);
                                final String s2 = new String(
                                                convertorToBytes.toBytes(key));
                                final String keyComapratorClassName = keyComparator
                                                .getClass().getSimpleName();
                                throw new IllegalArgumentException(String
                                                .format("Attempt to insers key in invalid order. "
                                                                + "Previous key is '%s', inserted key is '%s' and comparator is '%s'",
                                                                s1, s2,
                                                                keyComapratorClassName));
                        }
                }
                final byte[] keyBytes = convertorToBytes.toBytes(key);
                final int sharedByteLength = byteTool
                                .howMuchBytesIsSame(previousKeyBytes, keyBytes);
                final byte[] diffBytes = byteTool.getRemainingBytesAfterIndex(
                                sharedByteLength, keyBytes);

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
