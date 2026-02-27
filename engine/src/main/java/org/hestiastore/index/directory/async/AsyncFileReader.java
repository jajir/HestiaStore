package org.hestiastore.index.directory.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Vldtn;

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
     * Reads up to {@code length} bytes asynchronously into a destination
     * sub-range.
     *
     * @param bytes destination buffer
     * @param offset destination offset (inclusive)
     * @param length maximum number of bytes to read
     * @return completion supplying number of bytes read or {@code -1} on EOF
     */
    default CompletionStage<Integer> readAsync(final byte[] bytes,
            final int offset, final int length) {
        final byte[] validated = Vldtn.requireNonNull(bytes, "bytes");
        if (offset < 0 || length < 0 || offset > validated.length
                || offset + length > validated.length) {
            throw new IllegalArgumentException(String.format(
                    "Range [%d, %d) exceeds array length %d", offset,
                    offset + length, validated.length));
        }
        if (length == 0) {
            return CompletableFuture.completedFuture(Integer.valueOf(0));
        }
        if (offset == 0 && length == validated.length) {
            return readAsync(validated);
        }
        final byte[] temp = new byte[length];
        return readAsync(temp).thenApply(read -> {
            if (read > 0) {
                System.arraycopy(temp, 0, validated, offset, read);
            }
            return read;
        });
    }

    /**
     * Skips the specified number of bytes asynchronously.
     *
     * @param position bytes to skip forward
     * @return completion that finishes once the skip completes
     */
    CompletionStage<Void> skipAsync(long position);
}
