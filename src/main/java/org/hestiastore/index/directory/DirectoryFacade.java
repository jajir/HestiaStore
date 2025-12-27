package org.hestiastore.index.directory;

import java.util.stream.Stream;
import java.util.concurrent.CompletionStage;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncFileReader;
import org.hestiastore.index.directory.async.AsyncFileReaderSeekable;
import org.hestiastore.index.directory.async.AsyncFileWriter;

/**
 * Facade that exposes both synchronous {@link Directory} access and optional
 * asynchronous access via an {@link AsyncDirectory}. This allows callers to
 * migrate gradually from blocking to async IO while keeping a single injection
 * point.
 */
public final class DirectoryFacade {

    private final Directory directory;
    private final AsyncDirectory asyncDirectory;

    private DirectoryFacade(final Directory directory,
            final AsyncDirectory asyncDirectory) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.asyncDirectory = asyncDirectory;
    }

    /**
     * Creates a facade for synchronous-only access.
     */
    public static DirectoryFacade of(final Directory directory) {
        return new DirectoryFacade(directory, null);
    }

    /**
     * Creates a facade backed by both synchronous and asynchronous directories.
     */
    public static DirectoryFacade of(final Directory directory,
            final AsyncDirectory asyncDirectory) {
        return new DirectoryFacade(directory,
                Vldtn.requireNonNull(asyncDirectory, "asyncDirectory"));
    }

    public Directory getDirectory() {
        return directory;
    }

    public boolean hasAsync() {
        return asyncDirectory != null;
    }

    private AsyncDirectory requireAsync() {
        if (asyncDirectory == null) {
            throw new IllegalStateException(
                    "AsyncDirectory is not configured for this facade.");
        }
        return asyncDirectory;
    }

    /*
     * Synchronous delegates
     */

    public FileReader getFileReader(final String fileName) {
        return directory.getFileReader(fileName);
    }

    public FileReader getFileReader(final String fileName,
            final int bufferSize) {
        return directory.getFileReader(fileName, bufferSize);
    }

    public FileReaderSeekable getFileReaderSeekable(final String fileName) {
        return directory.getFileReaderSeekable(fileName);
    }

    public FileWriter getFileWriter(final String fileName) {
        return directory.getFileWriter(fileName);
    }

    public FileWriter getFileWriter(final String fileName,
            final Directory.Access access) {
        return directory.getFileWriter(fileName, access);
    }

    public FileWriter getFileWriter(final String fileName,
            final Directory.Access access, final int bufferSize) {
        return directory.getFileWriter(fileName, access, bufferSize);
    }

    public boolean isFileExists(final String fileName) {
        return directory.isFileExists(fileName);
    }

    public boolean deleteFile(final String fileName) {
        return directory.deleteFile(fileName);
    }

    public Stream<String> getFileNames() {
        return directory.getFileNames();
    }

    public void renameFile(final String currentFileName,
            final String newFileName) {
        directory.renameFile(currentFileName, newFileName);
    }

    public FileLock getLock(final String fileName) {
        return directory.getLock(fileName);
    }

    /*
     * Asynchronous delegates
     */

    public CompletionStage<AsyncFileReader> getFileReaderAsync(
            final String fileName) {
        return requireAsync().getFileReaderAsync(fileName);
    }

    public CompletionStage<AsyncFileReader> getFileReaderAsync(
            final String fileName, final int bufferSize) {
        return requireAsync().getFileReaderAsync(fileName, bufferSize);
    }

    public CompletionStage<AsyncFileReaderSeekable> getFileReaderSeekableAsync(
            final String fileName) {
        return requireAsync().getFileReaderSeekableAsync(fileName);
    }

    public CompletionStage<AsyncFileWriter> getFileWriterAsync(
            final String fileName) {
        return requireAsync().getFileWriterAsync(fileName);
    }

    public CompletionStage<AsyncFileWriter> getFileWriterAsync(
            final String fileName, final Directory.Access access) {
        return requireAsync().getFileWriterAsync(fileName, access);
    }

    public CompletionStage<AsyncFileWriter> getFileWriterAsync(
            final String fileName, final Directory.Access access,
            final int bufferSize) {
        return requireAsync().getFileWriterAsync(fileName, access, bufferSize);
    }

    public CompletionStage<Boolean> isFileExistsAsync(final String fileName) {
        return requireAsync().isFileExistsAsync(fileName);
    }

    public CompletionStage<Boolean> deleteFileAsync(final String fileName) {
        return requireAsync().deleteFileAsync(fileName);
    }

    public CompletionStage<Stream<String>> getFileNamesAsync() {
        return requireAsync().getFileNamesAsync();
    }

    public CompletionStage<Void> renameFileAsync(final String currentFileName,
            final String newFileName) {
        return requireAsync().renameFileAsync(currentFileName, newFileName);
    }

    public CompletionStage<FileLock> getLockAsync(final String fileName) {
        return requireAsync().getLockAsync(fileName);
    }
}

