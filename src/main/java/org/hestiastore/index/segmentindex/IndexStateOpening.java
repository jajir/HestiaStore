package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.FileLock;

public final class IndexStateOpening<K, V> implements IndexState<K, V> {

    private static final String LOCK_FILE_NAME = ".lock";

    private final FileLock fileLock;

    IndexStateOpening(final AsyncDirectory directoryFacade) {
        this.fileLock = Vldtn.requireNonNull(directoryFacade, "directoryFacade")
                .getLockAsync(LOCK_FILE_NAME).toCompletableFuture().join();
        if (fileLock.isLocked()) {
            throw new IllegalStateException(
                    "Index directory is already locked.");
        }
        fileLock.lock();
    }

    @Override
    public void onReady(final SegmentIndexImpl<K, V> index) {
        index.setIndexState(new IndexStateReady<>(fileLock));
    }

    @Override
    public void onClose(final SegmentIndexImpl<K, V> index) {
        throw new IllegalStateException("Can't close uninitialized index.");
    }

    @Override
    public void tryPerformOperation() {
        throw new IllegalStateException(
                "Can't perform operation while index is opening.");
    }

    FileLock getFileLock() {
        return fileLock;
    }
}
