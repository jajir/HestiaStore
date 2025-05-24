package com.hestiastore.index.log;

import java.util.Objects;

import com.hestiastore.index.datatype.TypeDescriptor;
import com.hestiastore.index.directory.Directory;

/**
 * Fluent builder for creating new instance of {@link LogImpl}.
 */
public class LogBuilder<K, V> {

    private Directory directory;

    private TypeDescriptor<K> keyTypeDescriptor;

    private TypeDescriptor<V> valueTypeDescriptor;

    public LogBuilder<K, V> withDirectory(final Directory directory) {
        this.directory = Objects.requireNonNull(directory);
        return this;
    }

    public LogBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        return this;
    }

    public LogBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        return this;
    }

    public LogImpl<K, V> build() {
        final LogFileNamesManager logFileNamesManager = new LogFileNamesManager(
                directory);
        final LogFilesManager<K, V> logFilesManager = new LogFilesManager<>(
                directory, new TypeDescriptorLoggedKey<>(keyTypeDescriptor),
                valueTypeDescriptor);
        final LogWriter<K, V> logWriter = new LogWriter<>(logFileNamesManager,
                logFilesManager);
        return new LogImpl<>(logWriter, logFileNamesManager, logFilesManager);
    }

    public LogEmptyImpl<K, V> buildEmpty() {
        return new LogEmptyImpl<>();
    }

}
