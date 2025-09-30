package org.hestiastore.index.unsorteddatafile;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.Directory.Access;
import org.hestiastore.index.directory.FileWriter;

public class UnsortedDataFileWriter<K, V> implements PairWriter<K, V> {

    private final TypeWriter<K> keyWriter;
    private final TypeWriter<V> valueWriter;
    private final FileWriter fileWriter;

    public UnsortedDataFileWriter(final Directory directory,
            final String fileName, final TypeWriter<K> keyWriter,
            final TypeWriter<V> valueWriter, final Access access,
            final int diskIoBufferSize) {
        this.keyWriter = Vldtn.requireNonNull(keyWriter, "keyWriter");
        this.valueWriter = Vldtn.requireNonNull(valueWriter, "valueWriter");
        Vldtn.requireNonNull(directory, "directory");
        Vldtn.requireNonNull(fileName, "fileName");
        fileWriter = directory.getFileWriter(fileName, access,
                diskIoBufferSize);
    }

    @Override
    public void write(final Pair<K, V> pair) {
        Vldtn.requireNonNull(pair, "pair");
        Vldtn.requireNonNull(pair.getKey(), "key");
        Vldtn.requireNonNull(pair.getValue(), "value");
        keyWriter.write(fileWriter, pair.getKey());
        valueWriter.write(fileWriter, pair.getValue());
    }

    @Override
    public void close() {
        fileWriter.close();
    }
}
