package org.hestiastore.index.log;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;

/**
 * Fluent builder for creating new instance of {@link LogImpl}.
 */
public final class LogBuilder<K, V> {

    private Directory directory;

    private TypeDescriptor<K> keyTypeDescriptor;

    private TypeDescriptor<V> valueTypeDescriptor;

    public LogBuilder<K, V> withDirectory(final Directory directory) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        return this;
    }

    public LogBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        return this;
    }

    public LogBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
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
        return new LogImpl<>(logWriter);
    }

    public LogEmptyImpl<K, V> buildEmpty() {
        return new LogEmptyImpl<>();
    }

}
