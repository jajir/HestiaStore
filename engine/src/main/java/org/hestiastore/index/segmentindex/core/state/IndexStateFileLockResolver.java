package org.hestiastore.index.segmentindex.core.state;

import org.hestiastore.index.directory.FileLock;

/**
 * Resolves the directory lock carried by the current index state, when one is
 * available.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class IndexStateFileLockResolver<K, V> {

    FileLock resolve(final IndexState<K, V> currentState) {
        if (currentState instanceof IndexStateReady<?, ?> readyState) {
            return readyState.getFileLock();
        }
        if (currentState instanceof IndexStateOpening<?, ?> openingState) {
            return openingState.getFileLock();
        }
        if (currentState instanceof IndexStateClosing<?, ?> closingState) {
            return closingState.getFileLock();
        }
        return null;
    }
}
