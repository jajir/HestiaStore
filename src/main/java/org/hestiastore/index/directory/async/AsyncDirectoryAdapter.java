package org.hestiastore.index.directory.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.directory.FileReaderSeekable;

/**
 * Default {@link AsyncDirectory} implementation that wraps a synchronous
 * {@link Directory} and routes all operations through a bounded executor.
 */
public final class AsyncDirectoryAdapter extends AbstractCloseableResource
        implements AsyncDirectory {

    private static final String IO_THREAD_NAME_PREFIX = "io";

    private final Directory delegate;
    private final ExecutorService executor;
    private final boolean shutdownExecutorOnClose;

    public static AsyncDirectory wrap(final Directory delegate,
            final int numberOfIoThreads) {
        final ExecutorService executor = Executors
                .newFixedThreadPool(numberOfIoThreads,
                        namedThreadFactory(IO_THREAD_NAME_PREFIX));
        return new AsyncDirectoryAdapter(delegate, executor, true);
    }

    public static AsyncDirectory wrap(final Directory delegate) {
        return wrap(delegate, 1);
    }

    public AsyncDirectoryAdapter(final Directory delegate,
            final ExecutorService executor,
            final boolean shutdownExecutorOnClose) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.executor = Vldtn.requireNonNull(executor, "executor");
        this.shutdownExecutorOnClose = shutdownExecutorOnClose;
    }

    @Override
    public CompletionStage<AsyncFileReader> getFileReaderAsync(
            final String fileName) {
        return supply(() -> new AsyncFileReaderAdapter(
                delegate.getFileReader(fileName), executor));
    }

    @Override
    public CompletionStage<AsyncFileReader> getFileReaderAsync(
            final String fileName, final int bufferSize) {
        return supply(() -> new AsyncFileReaderAdapter(
                delegate.getFileReader(fileName, bufferSize), executor));
    }

    @Override
    public CompletionStage<AsyncFileReaderSeekable> getFileReaderSeekableAsync(
            final String fileName) {
        return supply(() -> {
            final FileReaderSeekable fr = delegate.getFileReaderSeekable(
                    fileName);
            return new AsyncFileReaderSeekableAdapter(fr, executor);
        });
    }

    @Override
    public CompletionStage<AsyncFileWriter> getFileWriterAsync(
            final String fileName) {
        return getFileWriterAsync(fileName, Directory.Access.OVERWRITE);
    }

    @Override
    public CompletionStage<AsyncFileWriter> getFileWriterAsync(
            final String fileName, final Directory.Access access) {
        return supply(() -> new AsyncFileWriterAdapter(
                delegate.getFileWriter(fileName, access), executor));
    }

    @Override
    public CompletionStage<AsyncFileWriter> getFileWriterAsync(
            final String fileName, final Directory.Access access,
            final int bufferSize) {
        return supply(() -> new AsyncFileWriterAdapter(
                delegate.getFileWriter(fileName, access, bufferSize),
                executor));
    }

    @Override
    public CompletionStage<Boolean> isFileExistsAsync(final String fileName) {
        return supply(() -> Boolean.valueOf(delegate.isFileExists(fileName)));
    }

    @Override
    public CompletionStage<Boolean> deleteFileAsync(final String fileName) {
        return supply(() -> Boolean.valueOf(delegate.deleteFile(fileName)));
    }

    @Override
    public CompletionStage<Stream<String>> getFileNamesAsync() {
        return supply(() -> delegate.getFileNames());
    }

    @Override
    public CompletionStage<Void> renameFileAsync(final String currentFileName,
            final String newFileName) {
        return supply(() -> {
            delegate.renameFile(currentFileName, newFileName);
            return null;
        });
    }

    @Override
    public CompletionStage<AsyncDirectory> openSubDirectory(
            final String directoryName) {
        return supply(() -> new AsyncDirectoryAdapter(
                delegate.openSubDirectory(directoryName), executor, false));
    }

    @Override
    public CompletionStage<Boolean> rmdir(final String directoryName) {
        return supply(() -> Boolean.valueOf(delegate.rmdir(directoryName)));
    }

    @Override
    public CompletionStage<FileLock> getLockAsync(final String fileName) {
        return supply(() -> delegate.getLock(fileName));
    }

    private static ThreadFactory namedThreadFactory(final String prefix) {
        final AtomicInteger counter = new AtomicInteger(1);
        return runnable -> {
            final Thread thread = new Thread(runnable);
            thread.setName(prefix + "-" + counter.getAndIncrement());
            return thread;
        };
    }

    @Override
    protected void doClose() {
        if (shutdownExecutorOnClose) {
            executor.shutdown();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                executor.shutdownNow();
            }
        }
    }

    private <T> CompletionStage<T> supply(final Supplier<T> supplier) {
        if (wasClosed()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("AsyncDirectory already closed"));
        }
        return CompletableFuture.supplyAsync(() -> {
            if (wasClosed()) {
                throw new IllegalStateException(
                        "AsyncDirectory already closed");
            }
            return supplier.get();
        }, executor);
    }
}
