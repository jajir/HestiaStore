package com.coroptis.index.log;

import java.util.Objects;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeReader;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

public class LogImpl<K, V> implements Log<K, V> {

    /**
     * Log data will be stored in directory in filename 'filename' + '.' +
     * 'log'. For example 'log-0012.log'.
     */
    private final static String LOG_FILE_EXTENSION = ".log";

    private final UnsortedDataFile<LoggedKey<K>, V> log;

    public LogImpl(final Directory directory,
            final String fileName,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeWriter<V> valueWriter,
            final TypeReader<V> valueReader) {

        Objects.requireNonNull(directory);
        Objects.requireNonNull(fileName);
        Objects.requireNonNull(keyTypeDescriptor);
        Objects.requireNonNull(valueWriter);
        Objects.requireNonNull(valueReader);

        TypeDescriptorLoggedKey<K> tdlk = new TypeDescriptorLoggedKey<>(keyTypeDescriptor);

        this.log = new UnsortedDataFile<>(
                directory, fileName + LOG_FILE_EXTENSION, tdlk.getTypeWriter(), valueWriter,
                tdlk.getTypeReader(), valueReader);
    }

    public LogWriter<K, V> openWriter() {
        return new LogWriter<K, V>() {

            @Override
            public void delete(final K key, final V value) {
                // it's intentionally empty
            }

            @Override
            public void post(final K key, final V value) {
                // it's intentionally empty
            }

            @Override
            public void close() {
                // it's intentionally empty
            }

        };
    }

    public UnsortedDataFileStreamer<LoggedKey<K>, V> openStreamer() {
        return log.openStreamer();
    }
}
