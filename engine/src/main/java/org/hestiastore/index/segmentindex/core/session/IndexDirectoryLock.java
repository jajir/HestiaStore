package org.hestiastore.index.segmentindex.core.session;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;

/**
 * Owns the index directory lock for one live index session.
 */
final class IndexDirectoryLock {

    private static final String LOCK_FILE_NAME = ".lock";

    private final FileLock fileLock;
    private final boolean staleLockRecovered;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Acquires the index directory lock.
     *
     * @param directory managed index directory
     */
    IndexDirectoryLock(final Directory directory) {
        final Directory nonNullDirectory = Vldtn.requireNonNull(directory,
                "directory");
        final boolean lockFilePresent = nonNullDirectory
                .isFileExists(LOCK_FILE_NAME);
        this.fileLock = nonNullDirectory.getLock(LOCK_FILE_NAME);
        final boolean lockHeld = fileLock.isLocked();
        this.staleLockRecovered = lockFilePresent && !lockHeld;
        if (lockHeld) {
            throw new IllegalStateException(
                    "Index directory is already locked.");
        }
        fileLock.lock();
    }

    /**
     * @return {@code true} when a pre-existing lock marker was present but not
     *         actively held
     */
    boolean wasStaleLockRecovered() {
        return staleLockRecovered;
    }

    /**
     * Releases the directory lock once.
     */
    void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        fileLock.unlock();
    }
}
