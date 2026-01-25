package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.directory.async.AsyncDirectory;

/**
 * Coordinates acquisition and release of a segment directory lock.
 */
final class SegmentDirectoryLocking {

    private final AsyncDirectory directoryFacade;
    private final SegmentDirectoryLayout layout;
    private FileLock fileLock;

    SegmentDirectoryLocking(final AsyncDirectory directoryFacade,
            final SegmentDirectoryLayout layout) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.layout = Vldtn.requireNonNull(layout, "layout");
    }

    FileLock lock() {
        final String lockFileName = layout.getLockFileName();
        final FileLock lockHandle = directoryFacade.getLockAsync(lockFileName)
                .toCompletableFuture().join();
        if (lockHandle.isLocked()) {
            throw new IllegalStateException(lockHeldMessage(lockFileName));
        }
        try {
            lockHandle.lock();
        } catch (final IllegalStateException e) {
            throw new IllegalStateException(lockHeldMessage(lockFileName), e);
        }
        this.fileLock = lockHandle;
        return lockHandle;
    }

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
