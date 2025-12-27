package org.hestiastore.index.directory.async;

import java.util.concurrent.CompletionStage;

import org.hestiastore.index.CloseableResource;

/**
 * Asynchronous counterpart to {@link org.hestiastore.index.directory.FileWriter}
 * that executes blocking writes on a dedicated executor.
 */
public interface AsyncFileWriter extends CloseableResource {

    /**
     * Writes a single byte asynchronously.
     *
     * @param b byte to write
     * @return completion that finishes when the byte is persisted
     */
    CompletionStage<Void> writeAsync(byte b);

    /**
     * Writes an entire buffer asynchronously.
     *
     * @param bytes data to write
     * @return completion that finishes when the data is persisted
     */
    CompletionStage<Void> writeAsync(byte[] bytes);
}

