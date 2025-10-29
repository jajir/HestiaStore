package org.hestiastore.index.sorteddatafile;

import java.util.Comparator;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.F;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.FileWriter;

/**
 * Writes key-value pairs into sorted data file. Keys must be in ascending
 * order.
 * 
 * @param <K> key type
 * @param <V> value type
 */
public class SortedDataFileWriter<K, V> extends AbstractCloseableResource
        implements PairWriter<K, V> {

    private final TypeWriter<V> valueWriter;

    private final FileWriter fileWriter;

    private final Comparator<K> keyComparator;

    private final ConvertorToBytes<K> keyConvertorToBytes;

    private long position;

    private DiffKeyWriter<K> diffKeyWriter;

    private K previousKey = null;

    /**
     * Creates writer for sorted data file.
     * 
     * @param valueWriter       required value writer
     * @param fileWriter        required file writer
     * @param keyTypeDescriptor required key type descriptor
     */
    public SortedDataFileWriter(final TypeWriter<V> valueWriter,
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
                final String s2 = F
                        .b64(keyConvertorToBytes.toBytesBuffer(key).getData());
                final String keyComapratorClassName = keyComparator.getClass()
                        .getSimpleName();
                throw new IllegalArgumentException(String.format(
                        "Attempt to insert the same key as previous. Key(Base64)='%s', comparator='%s'",
                        s2, keyComapratorClassName));
            }
            if (cmp > 0) {
                final String s1 = F.b64(keyConvertorToBytes
                        .toBytesBuffer(previousKey).getData());
                final String s2 = F
                        .b64(keyConvertorToBytes.toBytesBuffer(key).getData());
                final String keyComapratorClassName = keyComparator.getClass()
                        .getSimpleName();
                throw new IllegalArgumentException(String.format(
                        "Attempt to insert key in invalid order. "
                                + "previous(Base64)='%s', inserted(Base64)='%s', comparator='%s'",
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

        final Bytes diffKey = diffKeyWriter.write(pair.getKey());
        fileWriter.write(diffKey);
        final int writenBytesInValue = valueWriter.write(fileWriter,
                pair.getValue());
        position = position + diffKey.length() + writenBytesInValue;
    }

    @Override
    protected void doClose() {
        fileWriter.close();
    }

}
