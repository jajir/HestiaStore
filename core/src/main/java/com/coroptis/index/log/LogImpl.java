package com.coroptis.index.log;

import java.util.Objects;

import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

public class LogImpl<K, V> implements Log<K, V> {

    private final LogWriter<K, V> logWriter;
    private final LogFileNamesManager logFileNamesManager;
    private final LogFilesManager<K, V> logFilesManager;

    public LogImpl(final LogWriter<K, V> logWriter,
            final LogFileNamesManager logFileNamesManager,
            final LogFilesManager<K, V> logFilesManager) {
        this.logWriter = Objects.requireNonNull(logWriter,
                "logWriter must not be null");
        this.logFileNamesManager = Objects.requireNonNull(logFileNamesManager,
                "logFileNamesManager must not be null");
        this.logFilesManager = Objects.requireNonNull(logFilesManager,
                "logFilesManager must not be null");
    }

    public UnsortedDataFileStreamer<LoggedKey<K>, V> openStreamer() {
        final UnsortedDataFileStreamer<LoggedKey<K>, V> streamer = new UnsortedDataFileStreamer<>(
                new LogFilesSpliterator<>(logFilesManager,
                        logFileNamesManager.getSortedLogFiles()));
        return streamer;
    }

    @Override
    public void rotate() {
        logWriter.rotate();
    }

    @Override
    public void post(final K key, final V value) {
        logWriter.post(key, value);
    }

    @Override
    public void delete(final K key, final V value) {
        logWriter.delete(key, value);
    }

    @Override
    public void close() {
        logWriter.close();
    }

}
