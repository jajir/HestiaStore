package org.hestiastore.index.directory.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.FileWriter;

/**
 * Synchronous {@link Directory} view that dispatches all work to a supplied
 * executor and blocks for completion.
 */
public final class AsyncDirectoryBlockingAdapter extends AbstractCloseableResource
        implements Directory {

    private final Directory delegate;
    private final ExecutorService executor;

    public AsyncDirectoryBlockingAdapter(final Directory delegate,
            final ExecutorService executor) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.executor = Vldtn.requireNonNull(executor, "executor");
    }

    @Override
    public FileReader getFileReader(final String fileName) {
        final AsyncFileReader reader = call(() -> new AsyncFileReaderAdapter(
                delegate.getFileReader(Vldtn.requireNonNull(fileName, "fileName")),
                executor));
        return new AsyncFileReaderBlockingAdapter(reader);
    }

    @Override
    public FileReader getFileReader(final String fileName,
            final int bufferSize) {
        final AsyncFileReader reader = call(() -> new AsyncFileReaderAdapter(
                delegate.getFileReader(Vldtn.requireNonNull(fileName, "fileName"),
                        bufferSize),
                executor));
        return new AsyncFileReaderBlockingAdapter(reader);
    }

    @Override
    public FileReaderSeekable getFileReaderSeekable(final String fileName) {
        final AsyncFileReaderSeekable reader = call(
                () -> new AsyncFileReaderSeekableAdapter(
                        delegate.getFileReaderSeekable(
                                Vldtn.requireNonNull(fileName, "fileName")),
                        executor));
        return new AsyncFileReaderSeekableBlockingAdapter(reader);
    }

    @Override
    public boolean isFileExists(final String fileName) {
        return call(() -> Boolean.valueOf(
                delegate.isFileExists(Vldtn.requireNonNull(fileName, "fileName"))))
                        .booleanValue();
    }

    @Override
    public FileWriter getFileWriter(final String fileName,
            final Access access) {
        final AsyncFileWriter writer = call(() -> new AsyncFileWriterAdapter(
                delegate.getFileWriter(Vldtn.requireNonNull(fileName, "fileName"),
                        Vldtn.requireNonNull(access, "access")),
                executor));
        return new AsyncFileWriterBlockingAdapter(writer);
    }

    @Override
    public FileWriter getFileWriter(final String fileName, final Access access,
            final int bufferSize) {
        final AsyncFileWriter writer = call(() -> new AsyncFileWriterAdapter(
                delegate.getFileWriter(Vldtn.requireNonNull(fileName, "fileName"),
                        Vldtn.requireNonNull(access, "access"), bufferSize),
                executor));
        return new AsyncFileWriterBlockingAdapter(writer);
    }

    @Override
    public boolean deleteFile(final String fileName) {
        return call(() -> Boolean.valueOf(
                delegate.deleteFile(Vldtn.requireNonNull(fileName, "fileName"))))
                        .booleanValue();
    }

    @Override
    public Stream<String> getFileNames() {
        return call(delegate::getFileNames);
    }

    @Override
    public void renameFile(final String currentFileName,
            final String newFileName) {
        run(() -> delegate.renameFile(
                Vldtn.requireNonNull(currentFileName, "currentFileName"),
                Vldtn.requireNonNull(newFileName, "newFileName")));
    }

    @Override
    public Directory openSubDirectory(final String directoryName) {
        final Directory subDirectory = call(() -> delegate.openSubDirectory(
                Vldtn.requireNonNull(directoryName, "directoryName")));
        return new AsyncDirectoryBlockingAdapter(subDirectory, executor);
    }

    @Override
    public boolean mkdir(final String directoryName) {
        return call(() -> Boolean.valueOf(
                delegate.mkdir(Vldtn.requireNonNull(directoryName, "directoryName"))))
                        .booleanValue();
    }

    @Override
    public boolean rmdir(final String directoryName) {
        return call(() -> Boolean.valueOf(
                delegate.rmdir(Vldtn.requireNonNull(directoryName, "directoryName"))))
                        .booleanValue();
    }

    @Override
    public FileLock getLock(final String fileName) {
        return call(() -> delegate.getLock(Vldtn.requireNonNull(fileName,
                "fileName")));
    }

    @Override
    protected void doClose() {
        // Executor lifecycle is owned by caller.
    }

    private <T> T call(final Supplier<T> supplier) {
        return CompletableFuture
                .supplyAsync(Vldtn.requireNonNull(supplier, "supplier"),
                        executor)
                .join();
    }

    private void run(final Runnable runnable) {
        CompletableFuture.runAsync(Vldtn.requireNonNull(runnable, "runnable"),
                executor).join();
    }
}
