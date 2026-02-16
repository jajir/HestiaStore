package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;

/**
 * Coordinates acquisition and release of a segment directory lock.
 */
final class SegmentDirectoryLocking {

    private final Directory directoryFacade;
    private final SegmentDirectoryLayout layout;
    private FileLock fileLock;

    SegmentDirectoryLocking(final Directory directoryFacade,
            final SegmentDirectoryLayout layout) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.layout = Vldtn.requireNonNull(layout, "layout");
    }

    /**
     * Attempts to acquire the segment lock.
     *
     * @return true when lock was acquired; false when lock is already held
     */
    boolean tryLock() {
        final String lockFileName = layout.getLockFileName();
        final FileLock lockHandle = directoryFacade.getLock(lockFileName);
        if (lockHandle.isLocked()) {
            return false;
        }
        try {
            lockHandle.lock();
        } catch (final IllegalStateException e) {
            return false;
        }
        this.fileLock = lockHandle;
        return true;
    }

    /**
     * Acquires the segment lock.
     *
     * @return acquired lock handle
     * @throws LockBusyException when the lock is already held
     */
    FileLock lock() {
        final String lockFileName = layout.getLockFileName();
        if (!tryLock()) {
            throw new LockBusyException(lockHeldMessage(lockFileName));
        }
        return fileLock;
    }

    /**
     * Releases the lock acquired by this helper.
     */
    void unlock() {
        if (fileLock != null && fileLock.isLocked()) {
            fileLock.unlock();
        }
    }

    private String lockHeldMessage(final String lockFileName) {
        return String.format(
                "Segment '%s' is already locked. Delete '%s' to recover.",
                layout.getSegmentId().getName(), lockFileName);
    }
}
