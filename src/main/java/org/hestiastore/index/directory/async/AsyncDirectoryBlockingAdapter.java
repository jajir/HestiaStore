package org.hestiastore.index.directory.async;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.FileWriter;

/**
 * Synchronous {@link Directory} view over {@link AsyncDirectory}.
 * <p>
 * Each operation delegates to the async directory and blocks until completion.
 * This is a compatibility adapter for callers that want a synchronous API while
 * still routing filesystem work through the async directory's bounded executor.
 * </p>
 */
public final class AsyncDirectoryBlockingAdapter extends AbstractCloseableResource
        implements Directory {

    private final AsyncDirectory delegate;
    private final boolean closeDelegateOnClose;

    public static AsyncDirectoryBlockingAdapter wrap(final Directory directory,
            final int numberOfIoThreads) {
        return new AsyncDirectoryBlockingAdapter(
                AsyncDirectoryAdapter.wrap(directory, numberOfIoThreads), true);
    }

    public static AsyncDirectoryBlockingAdapter wrap(final Directory directory,
            final ExecutorService executor) {
        return wrap(directory, executor, true);
    }

    public static AsyncDirectoryBlockingAdapter wrap(final Directory directory,
            final ExecutorService executor,
            final boolean shutdownExecutorOnClose) {
        return new AsyncDirectoryBlockingAdapter(new AsyncDirectoryAdapter(
                Vldtn.requireNonNull(directory, "directory"),
                Vldtn.requireNonNull(executor, "executor"),
                shutdownExecutorOnClose), true);
    }

    public static AsyncDirectoryBlockingAdapter wrap(final Directory directory) {
        return wrap(directory, 1);
    }

    public static AsyncDirectoryBlockingAdapter wrap(
            final AsyncDirectory asyncDirectory) {
        return new AsyncDirectoryBlockingAdapter(asyncDirectory, false);
    }

    public AsyncDirectoryBlockingAdapter(final AsyncDirectory delegate,
            final boolean closeDelegateOnClose) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.closeDelegateOnClose = closeDelegateOnClose;
    }

    /**
     * Returns the wrapped async directory delegate.
     *
     * @return wrapped async directory
     */
    public AsyncDirectory getAsyncDirectoryDelegate() {
        return delegate;
    }

    @Override
    public FileReader getFileReader(final String fileName) {
        final AsyncFileReader reader = await(delegate.getFileReaderAsync(
                Vldtn.requireNonNull(fileName, "fileName")));
        return new AsyncFileReaderBlockingAdapter(reader);
    }

    @Override
    public FileReader getFileReader(final String fileName,
            final int bufferSize) {
        final AsyncFileReader reader = await(delegate.getFileReaderAsync(
                Vldtn.requireNonNull(fileName, "fileName"), bufferSize));
        return new AsyncFileReaderBlockingAdapter(reader);
    }

    @Override
    public FileReaderSeekable getFileReaderSeekable(final String fileName) {
        final AsyncFileReaderSeekable reader = await(
                delegate.getFileReaderSeekableAsync(
                        Vldtn.requireNonNull(fileName, "fileName")));
        return new AsyncFileReaderSeekableBlockingAdapter(reader);
    }

    @Override
    public boolean isFileExists(final String fileName) {
        return await(delegate.isFileExistsAsync(
                Vldtn.requireNonNull(fileName, "fileName"))).booleanValue();
    }

    @Override
    public FileWriter getFileWriter(final String fileName,
            final Access access) {
        final AsyncFileWriter writer = await(delegate.getFileWriterAsync(
                Vldtn.requireNonNull(fileName, "fileName"),
                Vldtn.requireNonNull(access, "access")));
        return new AsyncFileWriterBlockingAdapter(writer);
    }

    @Override
    public FileWriter getFileWriter(final String fileName, final Access access,
            final int bufferSize) {
        final AsyncFileWriter writer = await(delegate.getFileWriterAsync(
                Vldtn.requireNonNull(fileName, "fileName"),
                Vldtn.requireNonNull(access, "access"), bufferSize));
        return new AsyncFileWriterBlockingAdapter(writer);
    }

    @Override
    public boolean deleteFile(final String fileName) {
        return await(delegate.deleteFileAsync(
                Vldtn.requireNonNull(fileName, "fileName"))).booleanValue();
    }

    @Override
    public Stream<String> getFileNames() {
        return await(delegate.getFileNamesAsync());
    }

    @Override
    public void renameFile(final String currentFileName,
            final String newFileName) {
        await(delegate.renameFileAsync(
                Vldtn.requireNonNull(currentFileName, "currentFileName"),
                Vldtn.requireNonNull(newFileName, "newFileName")));
    }

    @Override
    public Directory openSubDirectory(final String directoryName) {
        final AsyncDirectory subDirectory = await(delegate.openSubDirectory(
                Vldtn.requireNonNull(directoryName, "directoryName")));
        return new AsyncDirectoryBlockingAdapter(subDirectory, true);
    }

    @Override
    public boolean mkdir(final String directoryName) {
        final String requiredDirectoryName = Vldtn.requireNonNull(directoryName,
                "directoryName");
        final boolean existed;
        try (Stream<String> fileNames = await(delegate.getFileNamesAsync())) {
            existed = fileNames.anyMatch(requiredDirectoryName::equals);
        }
        await(delegate.openSubDirectory(requiredDirectoryName)).close();
        return !existed;
    }

    @Override
    public boolean rmdir(final String directoryName) {
        return await(delegate.rmdir(
                Vldtn.requireNonNull(directoryName, "directoryName")))
                        .booleanValue();
    }

    @Override
    public FileLock getLock(final String fileName) {
        return await(delegate.getLockAsync(
                Vldtn.requireNonNull(fileName, "fileName")));
    }

    @Override
    protected void doClose() {
        if (closeDelegateOnClose) {
            delegate.close();
        }
    }

    private static <T> T await(final CompletionStage<T> stage) {
        return Vldtn.requireNonNull(stage, "stage").toCompletableFuture().join();
    }
}
