package org.hestiastore.index.directory.async;

import java.util.concurrent.CompletionStage;

import org.hestiastore.index.CloseableResource;

/**
 * Asynchronous counterpart to {@link org.hestiastore.index.directory.FileReader}.
 * Implementations delegate blocking file IO to a dedicated executor so callers
 * can bound concurrent reads.
 */
public interface AsyncFileReader extends CloseableResource {

    /**
     * Reads a single byte asynchronously.
     *
     * @return completion supplying the byte value (0-255) or {@code -1} on EOF
     */
    CompletionStage<Integer> readAsync();

    /**
     * Reads into the given buffer asynchronously.
     *
     * @param bytes destination buffer
     * @return completion supplying number of bytes read or {@code -1} on EOF
     */
    CompletionStage<Integer> readAsync(byte[] bytes);

    /**
     * Skips the specified number of bytes asynchronously.
     *
     * @param position bytes to skip forward
     * @return completion that finishes once the skip completes
     */
    CompletionStage<Void> skipAsync(long position);
}

