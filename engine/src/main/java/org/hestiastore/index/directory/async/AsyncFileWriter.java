package org.hestiastore.index.directory.async;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Vldtn;

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

    /**
     * Writes a source array range asynchronously.
     *
     * @param bytes source bytes
     * @param offset start offset (inclusive)
     * @param length number of bytes to write
     * @return completion that finishes when the data is persisted
     */
    default CompletionStage<Void> writeAsync(final byte[] bytes,
            final int offset, final int length) {
        final byte[] validated = Vldtn.requireNonNull(bytes, "bytes");
        final int from = Vldtn.requireGreaterThanOrEqualToZero(offset,
                "offset");
        final int len = Vldtn.requireGreaterThanOrEqualToZero(length,
                "length");
        if (from > validated.length || from + len > validated.length) {
            throw new IllegalArgumentException(String.format(
                    "Offset '%s' and length '%s' exceed source length '%s'",
                    from, len, validated.length));
        }
        if (len == 0) {
            return CompletableFuture.completedFuture(null);
        }
        if (from == 0 && len == validated.length) {
            return writeAsync(validated);
        }
        return writeAsync(Arrays.copyOfRange(validated, from, from + len));
    }
}
