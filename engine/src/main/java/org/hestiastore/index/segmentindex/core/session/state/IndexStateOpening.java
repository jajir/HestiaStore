package org.hestiastore.index.segmentindex.core.session.state;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.segmentindex.SegmentIndexState;

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

    /**
     * Opens the index directory lock and records whether a stale lock marker
     * had to be recovered.
     *
     * @param directoryFacade managed index directory
     */
    public IndexStateOpening(final Directory directoryFacade) {
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

    /** {@inheritDoc} */
    @Override
    public SegmentIndexState state() {
        return SegmentIndexState.OPENING;
    }

    /**
     * Transitions the index to the ready state while keeping the acquired
     * directory lock.
     *
     * @return ready state holding the same directory lock
     */
    @Override
    public IndexState<K, V> onReady() {
        return new IndexStateReady<>(fileLock);
    }

    /**
     * Closing is not allowed while the index is still opening.
     *
     * @throws IllegalStateException always
     */
    @Override
    public IndexState<K, V> onClose() {
        throw new IllegalStateException("Can't close uninitialized index.");
    }

    /** {@inheritDoc} */
    @Override
    public IndexState<K, V> finishClose() {
        throw new IllegalStateException("Can't finish close while opening.");
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

    /**
     * @return {@code true} when a pre-existing lock marker was present but not
     *         actively held
     */
    public boolean wasStaleLockRecovered() {
        return staleLockRecovered;
    }
}
