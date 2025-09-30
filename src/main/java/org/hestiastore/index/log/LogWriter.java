package org.hestiastore.index.log;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.unsorteddatafile.UnsortedDataFile;

public class LogWriter<K, V> {

    private final LogFileNamesManager logFileNamessManager;
    private final LogFilesManager<K, V> logFilesManager;

    private LogUnsortedFileWriter<K, V> writer;

    LogWriter(final LogFileNamesManager logFileNamesManager,
            final LogFilesManager<K, V> logFilesManager) {
        this.logFileNamessManager = Vldtn.requireNonNull(logFileNamesManager,
                "logFileNamesManager");
        this.logFilesManager = Vldtn.requireNonNull(logFilesManager,
                "logFilesManager");
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
                    log.openWriterTx().openWriter());

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
