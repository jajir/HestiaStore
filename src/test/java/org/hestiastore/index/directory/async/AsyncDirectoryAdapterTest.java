package org.hestiastore.index.directory.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AsyncDirectoryAdapterTest {

    private ExecutorService executor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void round_trip_with_mem_directory() throws Exception {
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(new MemDirectory(), 2);
        final AsyncFileWriter writer = asyncDirectory.getFileWriterAsync("f")
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        writer.writeAsync("hi".getBytes(StandardCharsets.ISO_8859_1))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        writer.close();

        final AsyncFileReader reader = asyncDirectory.getFileReaderAsync("f")
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        final byte[] buffer = new byte[2];
        reader.readAsync(buffer).toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals("hi", new String(buffer, StandardCharsets.ISO_8859_1));
        reader.close();
        asyncDirectory.close();
    }

    @Test
    void operations_run_on_executor_thread() throws Exception {
        executor = Executors.newFixedThreadPool(1, r -> {
            final Thread t = new Thread(r);
            t.setName("io-thread");
            t.setDaemon(true);
            return t;
        });

        final RecordingFileWriter delegate = new RecordingFileWriter();
        final AsyncFileWriter writer = new AsyncFileWriterAdapter(delegate,
                executor);

        writer.writeAsync((byte) 1).toCompletableFuture().get(5,
                TimeUnit.SECONDS);
        assertTrue(delegate.lastThreadName.get().startsWith("io-thread"),
                "Write should execute on IO executor");
        writer.close();
    }

    @Test
    void rejects_operations_after_close() throws Exception {
        executor = Executors.newSingleThreadExecutor();
        final RecordingFileWriter delegate = new RecordingFileWriter();
        final AsyncFileWriter writer = new AsyncFileWriterAdapter(delegate,
                executor);
        writer.close();

        final CompletableFuture<Void> future = writer.writeAsync((byte) 2)
                .toCompletableFuture();
        final ExecutionException ex = assertThrows(ExecutionException.class,
                () -> future.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof IllegalStateException);
    }

    @Test
    void directory_rejects_new_requests_after_close() throws Exception {
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(new MemDirectory(), 1);
        asyncDirectory.close();
        final CompletableFuture<AsyncFileWriter> future = asyncDirectory
                .getFileWriterAsync("f").toCompletableFuture();
        final ExecutionException ex = assertThrows(ExecutionException.class,
                () -> future.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof IllegalStateException);
    }

    @Test
    void open_subdirectory_round_trip() throws Exception {
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(new MemDirectory(), 1);
        final AsyncDirectory subDirectory = asyncDirectory
                .openSubDirectory("child").toCompletableFuture().get(5,
                        TimeUnit.SECONDS);

        final AsyncFileWriter writer = subDirectory.getFileWriterAsync("f")
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        writer.writeAsync("hi".getBytes(StandardCharsets.ISO_8859_1))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        writer.close();

        final AsyncFileReader reader = subDirectory.getFileReaderAsync("f")
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        final byte[] buffer = new byte[2];
        reader.readAsync(buffer).toCompletableFuture().get(5,
                TimeUnit.SECONDS);
        assertEquals("hi", new String(buffer, StandardCharsets.ISO_8859_1));
        reader.close();

        subDirectory.close();
        asyncDirectory.getFileWriterAsync("root").toCompletableFuture().get(5,
                TimeUnit.SECONDS).close();
        asyncDirectory.close();
    }

    /**
     * Minimal {@link FileWriter} that records the thread name executing write
     * operations.
     */
    private static final class RecordingFileWriter
            extends AbstractCloseableResource implements FileWriter {

        final AtomicReference<String> lastThreadName = new AtomicReference<>();
        final AtomicReference<byte[]> lastBytes = new AtomicReference<>();

        @Override
        public void write(final byte b) {
            lastThreadName.set(Thread.currentThread().getName());
            lastBytes.set(new byte[] { b });
        }

        @Override
        public void write(final byte[] bytes) {
            lastThreadName.set(Thread.currentThread().getName());
            lastBytes.set(bytes);
        }

        @Override
        protected void doClose() {
            // no-op
        }
    }
}
