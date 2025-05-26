package com.hestiastore.index.log;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.hestiastore.index.directory.Directory;

/**
 * Responsible for managing log files.
 */
public final class LogFileNamesManager {
    private final static String LOG_FILE_EXTENSION = ".log";
    private final static String LOG_FILE_PREFIX = "wal-";
    private final static int MAX_LOG_FILE_NUMBER = 99999;

    private final Directory directory;

    public LogFileNamesManager(Directory directory) {
        this.directory = Objects.requireNonNull(directory);
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
                .filter(f -> f.startsWith(LOG_FILE_PREFIX))
                .filter(f -> f.endsWith(LOG_FILE_EXTENSION)).sorted()
                .collect(Collectors.toList());
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
