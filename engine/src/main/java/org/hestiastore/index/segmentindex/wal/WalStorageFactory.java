package org.hestiastore.index.segmentindex.wal;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.AbstractDirectory;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;

/**
 * Creates storage adapters for WAL runtime.
 */
final class WalStorageFactory {

    private WalStorageFactory() {
    }

    static WalStorage create(final Directory walDirectory) {
        final Directory directory = Vldtn.requireNonNull(walDirectory,
                "walDirectory");
        if (directory instanceof MemDirectory memDirectory) {
            return new WalStorageMem(memDirectory);
        }
        if (directory instanceof AbstractDirectory fsDirectory) {
            return new WalPathStorage(fsDirectory.path());
        }
        return new WalStorageDirectory(directory);
    }
}
