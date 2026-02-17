package org.hestiastore.index.directory;

import org.hestiastore.index.Vldtn;

public class FsFileLock implements FileLock {

    private final Directory directory;

    private final String lockFileName;

    FsFileLock(final Directory directory, final String lockFileName) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.lockFileName = Vldtn.requireNonNull(lockFileName, "lockFileName");
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
