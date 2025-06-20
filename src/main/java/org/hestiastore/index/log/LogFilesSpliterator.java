package org.hestiastore.index.log;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.hestiastore.index.CloseablePairReader;
import org.hestiastore.index.CloseableSpliterator;
import org.hestiastore.index.Pair;
import org.hestiastore.index.Vldtn;

/**
 * Allows to read log files.
 */
public class LogFilesSpliterator<K, V>
        implements CloseableSpliterator<LoggedKey<K>, V> {

    private final List<String> logFileNames;
    private final LogFilesManager<K, V> logFilesManager;
    private CloseablePairReader<LoggedKey<K>, V> pairReader;

    public LogFilesSpliterator(final LogFilesManager<K, V> logFilesManager,
            final List<String> logFileNames) {
        this.logFilesManager = Vldtn.requireNonNull(logFilesManager,
                "logFilesManager");
        this.logFileNames = new ArrayList<>(
                Vldtn.requireNonNull(logFileNames, "logFileNames"));
    }

    private void openNextFile() {
        if (pairReader != null) {
            pairReader.close();
        }
        if (logFileNames.isEmpty()) {
            pairReader = null;
        } else {
            final String fileName = logFileNames.remove(0);
            pairReader = logFilesManager.openReader(fileName);
        }
    }

    @Override
    public boolean tryAdvance(
            final Consumer<? super Pair<LoggedKey<K>, V>> action) {
        if (pairReader == null) {
            openNextFile();
            if (pairReader == null) {
                return false;
            }
        }
        final Pair<LoggedKey<K>, V> out = pairReader.read();
        if (out == null) {
            pairReader.close();
            pairReader = null;
            return tryAdvance(action);
        } else {
            action.accept(out);
            return true;
        }
    }

    @Override
    public Spliterator<Pair<LoggedKey<K>, V>> trySplit() {
        /*
         * It's not supported. So return null.
         */
        return null;
    }

    @Override
    public long estimateSize() {
        /*
         * Stream is not sized. It's not possible to determine stream size.
         */
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return Spliterator.DISTINCT | Spliterator.IMMUTABLE
                | Spliterator.NONNULL;
    }

    @Override
    public void close() {
        if (pairReader != null) {
            pairReader.close();
        }
    }

}
