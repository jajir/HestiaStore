package com.coroptis.index.log;

import java.util.Objects;

import com.coroptis.index.datatype.TypeReader;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.Directory;

/**
 * Unsorted key value pairs log file.
 * 
 * @author honza
 *
 * @param <K> key type
 * @param <V> value type
 */
public class LogImpl<K, V> {

    /**
     * Log data will be stored in directory in filename 'filename' + '.' +
     * 'log'. For example 'segment-00012.log'.
     */
    private final static String LOG_FILE_EXTENSION = "log";

    private final Directory directory;

    private final String fileName;

    private final TypeWriter<K> keyWriter;

    private final TypeWriter<V> valueWriter;

    private final TypeReader<K> keyReader;

    private final TypeReader<V> valueReader;

    public static <M, N> LogBuilder<M, N> builder() {
        return new LogBuilder<M, N>();
    }

    public LogImpl(final Directory directory, final String fileName,
            final TypeWriter<K> keyWriter, final TypeWriter<V> valueWriter,
            final TypeReader<K> keyReader, final TypeReader<V> valueReader) {
        this.directory = Objects.requireNonNull(directory);
        this.fileName = Objects.requireNonNull(fileName);
        this.keyWriter = Objects.requireNonNull(keyWriter);
        this.valueWriter = Objects.requireNonNull(valueWriter);
        this.keyReader = Objects.requireNonNull(keyReader);
        this.valueReader = Objects.requireNonNull(valueReader);
    }

}
