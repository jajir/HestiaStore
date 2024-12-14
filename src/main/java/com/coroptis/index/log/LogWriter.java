package com.coroptis.index.log;

import java.util.Objects;

import com.coroptis.index.directory.Directory.Access;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;

public class LogWriter<K, V> {

    private final LogFileNamesManager logFileNamessManager;
    private final LogFilesManager<K, V> logFilesManager;

    private LogUnsortedFileWriter<K, V> writer;

    LogWriter(final LogFileNamesManager logFileNamesManager,
            final LogFilesManager<K, V> logFilesManager) {
        this.logFileNamessManager = Objects.requireNonNull(logFileNamesManager);
        this.logFilesManager = Objects.requireNonNull(logFilesManager);
    }

    void post(final K key, final V value) {
        getWriter().post(key, value);
    }

    void delete(final K key, final V value) {
        getWriter().delete(key, value);
    }

    private LogUnsortedFileWriter<K, V> getWriter() {
        if (writer == null) {
            UnsortedDataFile<LoggedKey<K>, V> log = logFilesManager
                    .getLogFile(logFileNamessManager.getNewLogFileName());
            writer = new LogUnsortedFileWriterImpl<>(
                    log.openWriter(Access.OVERWRITE));
        }
        return writer;
    }

    void close() {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

    void rotate() {
        close();
    }

}
