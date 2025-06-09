package org.hestiastore.index.log;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.unsorteddatafile.UnsortedDataFileStreamer;

public class LogImpl<K, V> implements Log<K, V> {

    private final LogWriter<K, V> logWriter;
    private final LogFileNamesManager logFileNamesManager;
    private final LogFilesManager<K, V> logFilesManager;

    public LogImpl(final LogWriter<K, V> logWriter,
            final LogFileNamesManager logFileNamesManager,
            final LogFilesManager<K, V> logFilesManager) {
        this.logWriter = Vldtn.requireNonNull(logWriter, "logWriter");
        this.logFileNamesManager = Vldtn.requireNonNull(logFileNamesManager,
                "logFileNamesManager");
        this.logFilesManager = Vldtn.requireNonNull(logFilesManager,
                "logFilesManager");
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
