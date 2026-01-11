package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileLock;

public class IndexStateReady<K, V> implements IndexState<K, V> {

    private final FileLock fileLock;

    IndexStateReady(final FileLock fileLock) {
        this.fileLock = Vldtn.requireNonNull(fileLock, "fileLock");
    }

    @Override
    public void onReady(SegmentIndexImpl<K, V> index) {
        throw new IllegalStateException(
                "Can't make ready already ready index.");
    }

    @Override
    public void onClose(SegmentIndexImpl<K, V> index) {
        index.setIndexState(new IndexStateClosed<>());
        fileLock.unlock();
    }

    @Override
    public void tryPerformOperation() {
        // Do nothing operations are allowed.
    }

    FileLock getFileLock() {
        return fileLock;
    }
}
