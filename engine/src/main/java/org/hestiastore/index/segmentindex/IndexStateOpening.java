package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;

/**
 * Index state representing an opening index that holds the directory lock.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexStateOpening<K, V> implements IndexState<K, V> {

    private static final String LOCK_FILE_NAME = ".lock";

    private final FileLock fileLock;
    private final boolean staleLockRecovered;

    IndexStateOpening(final Directory directoryFacade) {
        final Directory nonNullDirectory = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
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
     * Transitions the index to the ready state while keeping the acquired
     * directory lock.
     *
     * @param index index instance
     */
    @Override
    public void onReady(final SegmentIndexImpl<K, V> index) {
        index.setIndexState(new IndexStateReady<>(fileLock));
    }

    /**
     * Closing is not allowed while the index is still opening.
     *
     * @param index index instance
     */
    @Override
    public void onClose(final SegmentIndexImpl<K, V> index) {
        throw new IllegalStateException("Can't close uninitialized index.");
    }

    /**
     * Rejects operations while the index is still opening.
     */
    @Override
    public void tryPerformOperation() {
        throw new IllegalStateException(
                "Can't perform operation while index is opening.");
    }

    FileLock getFileLock() {
        return fileLock;
    }

    boolean wasStaleLockRecovered() {
        return staleLockRecovered;
    }
}
