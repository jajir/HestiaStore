package org.hestiastore.index.sst;

import java.util.Objects;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;

public final class IndexStateNew<K, V> implements IndexState<K, V> {

    private final static String LOCK_FILE_NAME = ".lock";

    private final FileLock fileLock;

    IndexStateNew(final Directory directory) {
        this.fileLock = Objects.requireNonNull(directory)
                .getLock(LOCK_FILE_NAME);
        if (fileLock.isLocked()) {
            throw new IllegalStateException(
                    "Index directory is already locked.");
        }
        fileLock.lock();
    }

    @Override
    public void onReady(final SstIndexImpl<K, V> index) {
        index.setIndexState(new IndexStateReady<>(fileLock));
    }

    @Override
    public void onClose(final SstIndexImpl<K, V> index) {
        throw new IllegalStateException("Can't close uninitialized index.");
    }

    @Override
    public void tryPerformOperation() {
        throw new IllegalStateException(
                "Can't perform operation on uninitialized index.");
    }
}
