package org.hestiastore.index.log;

import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;

/**
 * Responsible for managing log files.
 */
public final class LogFileNamesManager {
    private static final String LOG_FILE_EXTENSION = ".log";
    private static final String LOG_FILE_PREFIX = "wal-";
    private static final int MAX_LOG_FILE_NUMBER = 99999;

    private final Directory directory;

    public LogFileNamesManager(Directory directory) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
    }

    String getNewLogFileName() {
        final List<String> fileNames = getSortedLogFiles();
        if (fileNames.isEmpty()) {
            return makeLogFileName(0);
        }
        int last = extractIndex(fileNames.get(fileNames.size() - 1));
        return makeLogFileName(last + 1);
    }

    List<String> getSortedLogFiles() {
        return directory.getFileNames()
                .filter(f -> f.startsWith(LOG_FILE_PREFIX))//
                .filter(f -> f.endsWith(LOG_FILE_EXTENSION))//
                .sorted()//
                .toList();
    }

    int extractIndex(final String fileName) {
        return Integer.parseInt(fileName.substring(LOG_FILE_PREFIX.length(),
                fileName.length() - LOG_FILE_EXTENSION.length()));
    }

    private String makeLogFileName(final int index) {
        if (index > MAX_LOG_FILE_NUMBER) {
            throw new IllegalStateException("Max number of log files reached");
        }
        String no = String.valueOf(index);
        while (no.length() < 5) {
            no = "0" + no;
        }
        return LOG_FILE_PREFIX + no + LOG_FILE_EXTENSION;
    }

}
