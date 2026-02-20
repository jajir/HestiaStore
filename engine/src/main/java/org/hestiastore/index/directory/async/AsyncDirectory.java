package org.hestiastore.index.directory.async;

import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;

/**
 * Asynchronous facade over {@link Directory} that routes blocking filesystem
 * operations through a dedicated executor to bound concurrent IO.
 */
public interface AsyncDirectory extends CloseableResource {

    CompletionStage<AsyncFileReader> getFileReaderAsync(String fileName);

    CompletionStage<AsyncFileReader> getFileReaderAsync(String fileName,
            int bufferSize);

    CompletionStage<AsyncFileReaderSeekable> getFileReaderSeekableAsync(
            String fileName);

    CompletionStage<AsyncFileWriter> getFileWriterAsync(String fileName);

    CompletionStage<AsyncFileWriter> getFileWriterAsync(String fileName,
            Directory.Access access);

    CompletionStage<AsyncFileWriter> getFileWriterAsync(String fileName,
            Directory.Access access, int bufferSize);

    CompletionStage<Boolean> isFileExistsAsync(String fileName);

    CompletionStage<Boolean> deleteFileAsync(String fileName);

    CompletionStage<Stream<String>> getFileNamesAsync();

    CompletionStage<Void> renameFileAsync(String currentFileName,
            String newFileName);

    /**
     * Opens a subdirectory for use, creating it if it does not exist.
     *
     * @param directoryName required subdirectory name
     * @return async directory handle for the subdirectory
     */
    CompletionStage<AsyncDirectory> openSubDirectory(String directoryName);

    /**
     * Removes an empty subdirectory in this directory.
     *
     * @param directoryName required subdirectory name
     * @return true if removed, false when it does not exist
     */
    CompletionStage<Boolean> rmdir(String directoryName);

    CompletionStage<FileLock> getLockAsync(String fileName);
}
