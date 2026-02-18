package org.hestiastore.index.directory.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AsyncDirectoryBlockingAdapterTest {

    private ExecutorService executor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void round_trip_with_mem_directory() {
        final AsyncDirectoryBlockingAdapter directory = AsyncDirectoryBlockingAdapter
                .wrap(new MemDirectory(), 2);
        try {
            final FileWriter writer = directory.getFileWriter("f");
            writer.write("hi".getBytes(StandardCharsets.ISO_8859_1));
            writer.close();

            final FileReader reader = directory.getFileReader("f");
            final byte[] buffer = new byte[2];
            reader.read(buffer);
            reader.close();
            assertEquals("hi", new String(buffer, StandardCharsets.ISO_8859_1));
        } finally {
            directory.close();
        }
    }

    @Test
    void mkdir_and_rmdir_are_supported() {
        final AsyncDirectoryBlockingAdapter directory = AsyncDirectoryBlockingAdapter
                .wrap(new MemDirectory(), 1);
        try {
            assertTrue(directory.mkdir("child"));
            assertFalse(directory.mkdir("child"));
            assertTrue(directory.rmdir("child"));
        } finally {
            directory.close();
        }
    }

    @Test
    void write_runs_on_async_delegate_executor() {
        executor = Executors.newSingleThreadExecutor(r -> {
            final Thread t = new Thread(r);
            t.setName("io-thread");
            t.setDaemon(true);
            return t;
        });
        final RecordingDirectory recordingDirectory = new RecordingDirectory();
        final AsyncDirectory asyncDirectory = new AsyncDirectoryAdapter(
                recordingDirectory, executor, false);
        final AsyncDirectoryBlockingAdapter directory = AsyncDirectoryBlockingAdapter
                .wrap(asyncDirectory);

        final FileWriter writer = directory.getFileWriter("f");
        writer.write((byte) 1);
        writer.close();

        assertTrue(recordingDirectory.lastWriteThreadName.get()
                .startsWith("io-thread"));
    }

    private static final class RecordingDirectory implements Directory {

        private final AtomicReference<String> lastWriteThreadName = new AtomicReference<>();

        @Override
        public FileReader getFileReader(final String fileName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileReader getFileReader(final String fileName,
                final int bufferSize) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileReaderSeekable getFileReaderSeekable(final String fileName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isFileExists(final String fileName) {
            return false;
        }

        @Override
        public FileWriter getFileWriter(final String fileName,
                final Access access) {
            return new RecordingFileWriter(lastWriteThreadName);
        }

        @Override
        public FileWriter getFileWriter(final String fileName,
                final Access access, final int bufferSize) {
            return new RecordingFileWriter(lastWriteThreadName);
        }

        @Override
        public boolean deleteFile(final String fileName) {
            return false;
        }

        @Override
        public Stream<String> getFileNames() {
            return Stream.empty();
        }

        @Override
        public void renameFile(final String currentFileName,
                final String newFileName) {
            // no-op
        }

        @Override
        public Directory openSubDirectory(final String directoryName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean mkdir(final String directoryName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rmdir(final String directoryName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileLock getLock(final String fileName) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class RecordingFileWriter
            extends AbstractCloseableResource implements FileWriter {

        private final AtomicReference<String> threadName;

        private RecordingFileWriter(final AtomicReference<String> threadName) {
            this.threadName = threadName;
        }

        @Override
        public void write(final byte b) {
            threadName.set(Thread.currentThread().getName());
        }

        @Override
        public void write(final byte[] bytes) {
            threadName.set(Thread.currentThread().getName());
        }

        @Override
        protected void doClose() {
            // no-op
        }
    }
}
