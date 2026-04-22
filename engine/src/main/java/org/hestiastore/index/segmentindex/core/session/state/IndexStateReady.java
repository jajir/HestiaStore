package org.hestiastore.index.segmentindex.core.session.state;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Index state representing a ready index with an active directory lock.
 *
 * @param <K> key type
 * @param <V> value type
 */
class IndexStateReady<K, V> implements IndexState<K, V> {

    private final FileLock fileLock;

    IndexStateReady(final FileLock fileLock) {
        this.fileLock = Vldtn.requireNonNull(fileLock, "fileLock");
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexState state() {
        return SegmentIndexState.READY;
    }

    /** {@inheritDoc} */
    @Override
    public IndexState<K, V> onReady() {
        throw new IllegalStateException(
                "Can't make ready already ready index.");
    }

    /** {@inheritDoc} */
    @Override
    public IndexState<K, V> onClose() {
        return new IndexStateClosing<>(fileLock);
    }

    /** {@inheritDoc} */
    @Override
    public IndexState<K, V> finishClose() {
        throw new IllegalStateException("Can't finish close from READY state.");
    }

    /** {@inheritDoc} */
    @Override
    public void tryPerformOperation() {
        // Do nothing operations are allowed.
    }

    FileLock getFileLock() {
        return fileLock;
    }
}
