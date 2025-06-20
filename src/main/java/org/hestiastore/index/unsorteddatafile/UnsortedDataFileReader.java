package org.hestiastore.index.unsorteddatafile;

import org.hestiastore.index.CloseablePairReader;
import org.hestiastore.index.Pair;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.directory.FileReader;

public class UnsortedDataFileReader<K, V> implements CloseablePairReader<K, V> {

    private final TypeReader<K> keyTypeReader;
    private final TypeReader<V> valueTypeReader;
    private final FileReader reader;

    UnsortedDataFileReader(final TypeReader<K> keyTypeReader,
            final TypeReader<V> valueTypeReader, final FileReader fileReader) {
        this.keyTypeReader = Vldtn.requireNonNull(keyTypeReader,
                "keyTypeReader");
        this.valueTypeReader = Vldtn.requireNonNull(valueTypeReader,
                "valueTypeReader");
        this.reader = Vldtn.requireNonNull(fileReader, "fileReader");
    }

    @Override
    public Pair<K, V> read() {
        final K key = keyTypeReader.read(reader);
        if (key == null) {
            return null;
        } else {
            final V value = valueTypeReader.read(reader);
            return new Pair<K, V>(key, value);
        }
    }

    @Override
    public void close() {
        reader.close();
    }

}
