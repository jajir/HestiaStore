package org.hestiastore.index.directory.async;

import java.util.concurrent.CompletionStage;

/**
 * Seekable asynchronous reader counterpart to
 * {@link org.hestiastore.index.directory.FileReaderSeekable}.
 */
public interface AsyncFileReaderSeekable extends AsyncFileReader {

    /**
     * Moves the cursor to the given position asynchronously.
     *
     * @param position zero-based offset inside the file
     * @return completion that finishes when positioning succeeds
     */
    CompletionStage<Void> seekAsync(long position);
}

