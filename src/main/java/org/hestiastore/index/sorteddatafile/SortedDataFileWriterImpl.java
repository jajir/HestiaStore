package org.hestiastore.index.sorteddatafile;

import java.util.Comparator;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.FileWriter;

public class SortedDataFileWriterImpl<K, V> implements PairWriter<K, V> {

    private final TypeWriter<V> valueWriter;

    private final FileWriter fileWriter;

    private final Comparator<K> keyComparator;

    private final ConvertorToBytes<K> keyConvertorToBytes;

    private long position;

    private DiffKeyWriter<K> diffKeyWriter;

    private K previousKey = null;

    public SortedDataFileWriterImpl(final TypeWriter<V> valueWriter,
            final FileWriter fileWriter,
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.valueWriter = Vldtn.requireNonNull(valueWriter, "valueWriter");
        this.fileWriter = Vldtn.requireNonNull(fileWriter, "fileWriter");
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        this.keyComparator = Vldtn.requireNonNull(
                keyTypeDescriptor.getComparator(), "keyComparator");
        this.keyConvertorToBytes = Vldtn.requireNonNull(
                keyTypeDescriptor.getConvertorToBytes(), "keyConvertorToBytes");
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
                final String s2 = new String(keyConvertorToBytes.toBytes(key));
                final String keyComapratorClassName = keyComparator.getClass()
                        .getSimpleName();
                throw new IllegalArgumentException(String.format(
                        "Attempt to insers same key as previous. Key is '%s' and comparator is '%s'",
                        s2, keyComapratorClassName));
            }
            if (cmp > 0) {
                final String s1 = new String(
                        keyConvertorToBytes.toBytes(previousKey));
                final String s2 = new String(keyConvertorToBytes.toBytes(key));
                final String keyComapratorClassName = keyComparator.getClass()
                        .getSimpleName();
                throw new IllegalArgumentException(String.format(
                        "Attempt to insers key in invalid order. "
                                + "Previous key is '%s', inserted key is '%s' and comparator is '%s'",
                        s1, s2, keyComapratorClassName));
            }
        }
        previousKey = key;
    }

    /**
     * Writes the given key-value pair.
     *
     * @param pair required key-value pair
     */
    @Override
    public void write(final Pair<K, V> pair) {
        Vldtn.requireNonNull(pair, "pair");
        Vldtn.requireNonNull(pair.getKey(), "key");
        Vldtn.requireNonNull(pair.getValue(), "value");
        verifyKeyOrder(pair.getKey());

        final byte[] diffKey = diffKeyWriter.write(pair.getKey());
        fileWriter.write(diffKey);
        final int writenBytesInValue = valueWriter.write(fileWriter,
                pair.getValue());
        position = position + diffKey.length + writenBytesInValue;
    }

    @Override
    public void close() {
        fileWriter.close();
    }

}
