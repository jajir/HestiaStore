package org.hestiastore.index.segmentindex.wal;

import java.util.stream.Stream;

/**
 * Internal filesystem abstraction used by WAL runtime.
 */
interface WalStorage {

    boolean exists(String fileName);

    void touch(String fileName);

    long size(String fileName);

    void append(String fileName, byte[] bytes, int offset, int length);

    void overwrite(String fileName, byte[] bytes, int offset, int length);

    byte[] readAll(String fileName);

    int read(String fileName, long position, byte[] destination, int offset,
            int length);

    void truncate(String fileName, long sizeBytes);

    boolean delete(String fileName);

    void rename(String currentFileName, String newFileName);

    Stream<String> listFileNames();

    /**
     * Persists buffered data/metadata for the given file to stable storage.
     *
     * <p>
     * Implementations that cannot provide fsync semantics (for example in-memory
     * stores) should implement this as a no-op.
     * </p>
     *
     * @param fileName file to sync
     */
    void sync(String fileName);

    /**
     * Persists WAL directory metadata changes (create/rename/delete) to stable
     * storage.
     *
     * <p>
     * Implementations that cannot provide metadata fsync semantics should
     * implement this as a no-op.
     * </p>
     */
    void syncMetadata();
}
