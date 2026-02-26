package org.hestiastore.index.sorteddatafile;

import java.util.Comparator;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.F;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeEncoder;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.FileWriter;

/**
 * Writes key-value entries into sorted data file. Keys must be in ascending
 * order.
 * 
 * @param <K> key type
 * @param <V> value type
 */
public class SortedDataFileWriter<K, V> extends AbstractCloseableResource
        implements EntryWriter<K, V> {

    private final TypeWriter<V> valueWriter;

    private final FileWriter fileWriter;

    private final Comparator<K> keyComparator;

    private final TypeEncoder<K> keyTypeEncoder;

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
        this.keyTypeEncoder = Vldtn.requireNonNull(
                keyTypeDescriptor.getTypeEncoder(), "keyTypeEncoder");
        this.diffKeyWriter = makeDiffKeyWriter();
        position = 0;
    }

    /**
     * Creates a diff-key writer for the configured key type.
     *
     * @return diff-key writer instance
     */
    private DiffKeyWriter<K> makeDiffKeyWriter() {
        return new DiffKeyWriter<>(keyTypeEncoder, keyComparator);
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
                final String s2 = encodeKeyToBase64(key);
                final String keyComapratorClassName = keyComparator.getClass()
                        .getSimpleName();
                throw new IllegalArgumentException(String.format(
                        "Attempt to insert the same key as previous. Key(Base64)='%s', comparator='%s'",
                        s2, keyComapratorClassName));
            }
            if (cmp > 0) {
                final String s1 = encodeKeyToBase64(previousKey);
                final String s2 = encodeKeyToBase64(key);
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

    private String encodeKeyToBase64(final K key) {
        final int encodedKeyLength = Vldtn.requireGreaterThanOrEqualToZero(
                keyTypeEncoder.bytesLength(key), "encodedKeyLength");
        final byte[] keyBytes = new byte[encodedKeyLength];
        final int writtenBytes = keyTypeEncoder.toBytes(key, keyBytes);
        if (writtenBytes != encodedKeyLength) {
            throw new IllegalStateException(String.format(
                    "Encoder wrote '%s' bytes but declared '%s'", writtenBytes,
                    encodedKeyLength));
        }
        return F.b64(keyBytes);
    }

    /**
     * Writes the given key-value entry.
     *
     * @param entry required key-value entry
     */
    @Override
    public void write(final Entry<K, V> entry) {
        Vldtn.requireNonNull(entry, "entry");
        Vldtn.requireNonNull(entry.getKey(), "key");
        Vldtn.requireNonNull(entry.getValue(), "value");
        verifyKeyOrder(entry.getKey());

        final byte[] diffKey = diffKeyWriter.write(entry.getKey());
        fileWriter.write(diffKey);
        final int writenBytesInValue = valueWriter.write(fileWriter,
                entry.getValue());
        position = position + diffKey.length + writenBytesInValue;
    }

    /**
     * Closes the underlying file writer.
     */
    @Override
    protected void doClose() {
        fileWriter.close();
    }

}
