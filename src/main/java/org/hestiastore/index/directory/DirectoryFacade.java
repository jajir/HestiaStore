package org.hestiastore.index.directory;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.directory.async.AsyncFileReader;
import org.hestiastore.index.directory.async.AsyncFileReaderSeekable;
import org.hestiastore.index.directory.async.AsyncFileWriter;

/**
 * Facade that exposes both synchronous {@link Directory} access and
 * asynchronous access via an {@link AsyncDirectory}. This allows callers to
 * migrate gradually from blocking to async IO while keeping a single injection
 * point.
 */
public final class DirectoryFacade {

    private static final int DEFAULT_ASYNC_IO_THREADS = 1;
    private static final ExecutorService DEFAULT_ASYNC_EXECUTOR = Executors
            .newFixedThreadPool(DEFAULT_ASYNC_IO_THREADS, r -> {
                final Thread thread = new Thread(r);
                thread.setName("hestia-async-io");
                thread.setDaemon(true);
                return thread;
            });

    private final Directory directory;
    private final AsyncDirectory asyncDirectory;

    private DirectoryFacade(final Directory directory,
            final AsyncDirectory asyncDirectory) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.asyncDirectory = Vldtn.requireNonNull(asyncDirectory,
                "asyncDirectory");
    }

    /**
     * Creates a facade for synchronous access with a default asynchronous
     * adapter.
     */
    public static DirectoryFacade of(final Directory directory) {
        final Directory safeDirectory = Vldtn.requireNonNull(directory,
                "directory");
        return new DirectoryFacade(safeDirectory,
                new AsyncDirectoryAdapter(safeDirectory,
                        DEFAULT_ASYNC_EXECUTOR,
                        false));
    }

    /**
     * Creates a facade backed by both synchronous and asynchronous directories.
     */
    public static DirectoryFacade of(final Directory directory,
            final AsyncDirectory asyncDirectory) {
        return new DirectoryFacade(directory,
                Vldtn.requireNonNull(asyncDirectory, "asyncDirectory"));
    }

    // internal accessor retained for legacy adapters; prefer direct delegates
    Directory getDirectoryInternal() {
        return directory;
    }

    public boolean hasAsync() {
        return true;
    }

    /*
     * Synchronous delegates
     */

    @Deprecated
    public FileReader getFileReader(final String fileName) {
        return directory.getFileReader(fileName);
    }

    @Deprecated
    public FileReader getFileReader(final String fileName,
            final int bufferSize) {
        return directory.getFileReader(fileName, bufferSize);
    }

    @Deprecated
    public FileReaderSeekable getFileReaderSeekable(final String fileName) {
        return directory.getFileReaderSeekable(fileName);
    }

    @Deprecated
    public FileWriter getFileWriter(final String fileName) {
        return directory.getFileWriter(fileName);
    }

    @Deprecated
    public FileWriter getFileWriter(final String fileName,
            final Directory.Access access) {
        return directory.getFileWriter(fileName, access);
    }

    @Deprecated
    public FileWriter getFileWriter(final String fileName,
            final Directory.Access access, final int bufferSize) {
        return directory.getFileWriter(fileName, access, bufferSize);
    }

    @Deprecated
    public boolean isFileExists(final String fileName) {
        return directory.isFileExists(fileName);
    }

    @Deprecated
    public boolean deleteFile(final String fileName) {
        return directory.deleteFile(fileName);
    }

    @Deprecated
    public Stream<String> getFileNames() {
        return directory.getFileNames();
    }

    @Deprecated
    public void renameFile(final String currentFileName,
            final String newFileName) {
        directory.renameFile(currentFileName, newFileName);
    }

    @Deprecated
    public FileLock getLock(final String fileName) {
        return directory.getLock(fileName);
    }

    /*
     * Asynchronous delegates
     */

    public CompletionStage<AsyncFileReader> getFileReaderAsync(
            final String fileName) {
        return asyncDirectory.getFileReaderAsync(fileName);
    }

    public CompletionStage<AsyncFileReader> getFileReaderAsync(
            final String fileName, final int bufferSize) {
        return asyncDirectory.getFileReaderAsync(fileName, bufferSize);
    }

    public CompletionStage<AsyncFileReaderSeekable> getFileReaderSeekableAsync(
            final String fileName) {
        return asyncDirectory.getFileReaderSeekableAsync(fileName);
    }

    public CompletionStage<AsyncFileWriter> getFileWriterAsync(
            final String fileName) {
        return asyncDirectory.getFileWriterAsync(fileName);
    }

    public CompletionStage<AsyncFileWriter> getFileWriterAsync(
            final String fileName, final Directory.Access access) {
        return asyncDirectory.getFileWriterAsync(fileName, access);
    }

    public CompletionStage<AsyncFileWriter> getFileWriterAsync(
            final String fileName, final Directory.Access access,
            final int bufferSize) {
        return asyncDirectory.getFileWriterAsync(fileName, access, bufferSize);
    }

    public CompletionStage<Boolean> isFileExistsAsync(final String fileName) {
        return asyncDirectory.isFileExistsAsync(fileName);
    }

    public CompletionStage<Boolean> deleteFileAsync(final String fileName) {
        return asyncDirectory.deleteFileAsync(fileName);
    }

    public CompletionStage<Stream<String>> getFileNamesAsync() {
        return asyncDirectory.getFileNamesAsync();
    }

    public CompletionStage<Void> renameFileAsync(final String currentFileName,
            final String newFileName) {
        return asyncDirectory.renameFileAsync(currentFileName, newFileName);
    }

    public CompletionStage<FileLock> getLockAsync(final String fileName) {
        return asyncDirectory.getLockAsync(fileName);
    }
}
