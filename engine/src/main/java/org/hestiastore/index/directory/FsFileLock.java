package org.hestiastore.index.directory;

import org.hestiastore.index.Vldtn;

public class FsFileLock implements FileLock {

    private final Directory directory;

    private final String lockFileName;
    private FileLockMetadata ownedMetadata;

    FsFileLock(final Directory directory, final String lockFileName) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.lockFileName = Vldtn.requireNonNull(lockFileName, "lockFileName");
    }

    @Override
    public boolean isLocked() {
        if (!directory.isFileExists(lockFileName)) {
            return false;
        }
        final FileLockMetadata lockMetadata = FileLockMetadata
                .read(directory, lockFileName).orElse(null);
        if (lockMetadata == null) {
            return true;
        }
        if (lockMetadata.canRecoverAsStale()) {
            directory.deleteFile(lockFileName);
            return false;
        }
        return true;
    }

    @Override
    public void lock() {
        if (isLocked()) {
            throw new IllegalStateException(String.format(
                    "Can't lock already locked file '%s'.", lockFileName));
        }
        final FileLockMetadata metadata = FileLockMetadata.currentProcess();
        metadata.write(directory, lockFileName);
        ownedMetadata = metadata;
    }

    @Override
    public void unlock() {
        if (!directory.isFileExists(lockFileName)) {
            throw new IllegalStateException(String.format(
                    "Can't unlock already unlocked file '%s'.", lockFileName));
        }
        if (ownedMetadata != null) {
            final FileLockMetadata currentMetadata = FileLockMetadata
                    .read(directory, lockFileName).orElse(null);
            if (currentMetadata != null
                    && !ownedMetadata.hasSameSession(currentMetadata)) {
                throw new IllegalStateException(String.format(
                        "Can't unlock file '%s' owned by another process.",
                        lockFileName));
            }
        }
        directory.deleteFile(lockFileName);
        ownedMetadata = null;
    }

}
