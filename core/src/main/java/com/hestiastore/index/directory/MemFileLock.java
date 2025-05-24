package com.hestiastore.index.directory;

import java.util.Objects;

public class MemFileLock implements FileLock {

    private final MemDirectory directory;

    private final String lockFileName;

    MemFileLock(final MemDirectory directory, final String lockFileName) {
        this.directory = Objects.requireNonNull(directory);
        this.lockFileName = Objects.requireNonNull(lockFileName);
    }

    @Override
    public boolean isLocked() {
        return directory.isFileExists(lockFileName);
    }

    @Override
    public void lock() {
        if (isLocked()) {
            throw new IllegalStateException(String.format(
                    "Can't lock already locked file '%s'.", lockFileName));
        }
        try (FileWriter writer = directory.getFileWriter(lockFileName)) {
            writer.write((byte) 0xFF);
        }
    }

    @Override
    public void unlock() {
        if (!isLocked()) {
            throw new IllegalStateException(String.format(
                    "Can't unlock already unlocked file '%s'.", lockFileName));
        }
        directory.deleteFile(lockFileName);
    }

}
