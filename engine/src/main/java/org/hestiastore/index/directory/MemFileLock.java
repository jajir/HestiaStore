package org.hestiastore.index.directory;

import org.hestiastore.index.Vldtn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemFileLock implements FileLock {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final MemDirectory directory;

    private final String lockFileName;
    private FileLockMetadata ownedMetadata;

    MemFileLock(final MemDirectory directory, final String lockFileName) {
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
            if (lockMetadata.getPid() == ProcessHandle.current().pid()) {
                logger.info(
                        "Recovered stale lock '{}' with same pid '{}'. Index is going to be checked for consistency and unlocked.",
                        lockFileName, lockMetadata.getPid());
            }
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
