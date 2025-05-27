package org.hestiastore.index.sst;

import java.util.Objects;

import org.hestiastore.index.directory.FileLock;

public class IndexStateReady<K, V> implements IndexState<K, V> {

    private final FileLock fileLock;

    IndexStateReady(final FileLock fileLock) {
        this.fileLock = Objects.requireNonNull(fileLock);
    }

    @Override
    public void onReady(SstIndexImpl<K, V> index) {
        throw new IllegalStateException(
                "Can't make ready already ready index.");
    }

    @Override
    public void onClose(SstIndexImpl<K, V> index) {
        index.setIndexState(new IndexStateClose<>());
        fileLock.unlock();
    }

    @Override
    public void tryPerformOperation() {
        // Do nothing operations are allowed.
    }
}
