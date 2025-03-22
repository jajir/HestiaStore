package com.coroptis.index.sorteddatafile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.datatype.ConvertorToBytes;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.FileWriter;

public class SortedDataFileWriter<K, V> implements CloseableResource {

        private final TypeWriter<V> valueWriter;

        private final FileWriter fileWriter;

        private final TypeDescriptor<K> keyTypeDescriptor;

        private final Comparator<K> keyComparator;

        private final ConvertorToBytes<K> keyConvertorToBytes;

        private long position;

        private DiffKeyWriter<K> diffKeyWriter;

        private K previousKey = null;

        public SortedDataFileWriter(final TypeWriter<V> valueWriter,
                        final FileWriter fileWriter,
                        final TypeDescriptor<K> keyTypeDescriptor) {
                this.valueWriter = Objects.requireNonNull(valueWriter,
                                "valueWriter is required");
                this.fileWriter = Objects.requireNonNull(fileWriter,
                                "fileWriter is required");
                this.keyTypeDescriptor = Objects.requireNonNull(
                                keyTypeDescriptor,
                                "keyTypeDescriptor is required");
                this.keyComparator = Objects.requireNonNull(
                                keyTypeDescriptor.getComparator(),
                                "Comparator is required");
                this.keyConvertorToBytes = Objects.requireNonNull(
                                keyTypeDescriptor.getConvertorToBytes(),
                                "Convertor to bytes is required");
                this.diffKeyWriter = makeDiffKeyWriter();
                position = 0;
        }

        private DiffKeyWriter<K> makeDiffKeyWriter() {
                return new DiffKeyWriter<>(keyConvertorToBytes, keyComparator);
        }

        /**
         * Verify that keys are in correct order.
         * 
         * @param key
         */
        private void verifyKeyOrder(final K key) {
                if (previousKey != null) {
                        final int cmp = keyComparator.compare(previousKey, key);
                        if (cmp == 0) {
                                final String s2 = new String(keyConvertorToBytes
                                                .toBytes(key));
                                final String keyComapratorClassName = keyComparator
                                                .getClass().getSimpleName();
                                throw new IllegalArgumentException(String
                                                .format("Attempt to insers same key as previous. Key '%s' was comapred with '%s'",
                                                                s2,
                                                                keyComapratorClassName));
                        }
                        if (cmp > 0) {
                                final String s1 = new String(keyConvertorToBytes
                                                .toBytes(previousKey));
                                final String s2 = new String(keyConvertorToBytes
                                                .toBytes(key));
                                final String keyComapratorClassName = keyComparator
                                                .getClass().getSimpleName();
                                throw new IllegalArgumentException(String
                                                .format("Attempt to insers key in invalid order. "
                                                                + "Previous key is '%s', inserted key is '%s' and comparator is '%s'",
                                                                s1, s2,
                                                                keyComapratorClassName));
                        }
                }
        }

        /**
         * Allows to put new key value pair into index.
         *
         * @param pair      required key value pair
         * @param fullWrite when it's <code>true</code> than key is written
         *                  whole without shared part with previous key.
         * @return position of end of record.
         */
        private long put(final Pair<K, V> pair, final boolean fullWrite) {
                Objects.requireNonNull(pair, "pair is required");
                Objects.requireNonNull(pair.getKey(), "key is required");
                Objects.requireNonNull(pair.getValue(), "value is required");
                verifyKeyOrder(pair.getKey());
                previousKey = pair.getKey();

                if (fullWrite) {
                        diffKeyWriter = makeDiffKeyWriter();
                }

                final byte[] diffKey = diffKeyWriter.write(pair.getKey());
                fileWriter.write(diffKey);
                final int writenBytesInValue = valueWriter.write(fileWriter,
                                pair.getValue());

                long lastPosition = position;
                position = position + diffKey.length + writenBytesInValue;
                return lastPosition;
        }

        /**
         * Writes the given key-value pair.
         *
         * @param pair required key-value pair
         */
        public void write(final Pair<K, V> pair) {
                put(pair, false);
        }

        /**
         * Writes the given key-value pair, forcing all data to be written.
         *
         * @param pair required key-value pair
         * @return position where will next data starts
         */
        public long writeFull(final Pair<K, V> pair) {
                return put(pair, true);
        }

        @Override
        public void close() {
                fileWriter.close();
        }

}
