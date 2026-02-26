package org.hestiastore.index.directory;

import java.util.function.Supplier;

/**
 * Supplier of seekable readers whose lifecycle can be closed when no longer
 * needed.
 */
@FunctionalInterface
public interface FileReaderSeekableSupplier
        extends Supplier<FileReaderSeekable>, AutoCloseable {

    @Override
    default void close() {
        // default no-op
    }
}
