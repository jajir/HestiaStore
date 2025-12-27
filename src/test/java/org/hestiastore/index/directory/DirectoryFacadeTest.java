package org.hestiastore.index.directory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.directory.async.AsyncFileReader;
import org.hestiastore.index.directory.async.AsyncFileWriter;
import org.junit.jupiter.api.Test;

class DirectoryFacadeTest {

    @Test
    void synchronous_delegation_with_mem_directory() throws Exception {
        final MemDirectory memDirectory = new MemDirectory();
        final DirectoryFacade facade = DirectoryFacade.of(memDirectory);

        assertFalse(facade.isFileExists("a"));
        try (FileWriter writer = facade.getFileWriter("a")) {
            writer.write("hi".getBytes(StandardCharsets.ISO_8859_1));
        }
        assertTrue(facade.isFileExists("a"));

        try (FileReader reader = facade.getFileReader("a")) {
            final byte[] buffer = new byte[2];
            reader.read(buffer);
            assertEquals("hi", new String(buffer, StandardCharsets.ISO_8859_1));
        }
    }

    @Test
    void async_delegation_when_configured() throws Exception {
        final MemDirectory memDirectory = new MemDirectory();
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(memDirectory, 2);
        final DirectoryFacade facade = DirectoryFacade.of(memDirectory,
                asyncDirectory);

        final AsyncFileWriter writer = facade.getFileWriterAsync("b")
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        writer.writeAsync("yo".getBytes(StandardCharsets.ISO_8859_1))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        writer.close();

        final AsyncFileReader reader = facade.getFileReaderAsync("b")
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        final byte[] buffer = new byte[2];
        reader.readAsync(buffer).toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
        assertEquals("yo", new String(buffer, StandardCharsets.ISO_8859_1));
        reader.close();
    }

    @Test
    void async_methods_reject_when_async_not_configured() {
        final DirectoryFacade facade = DirectoryFacade.of(new MemDirectory());
        assertThrows(IllegalStateException.class,
                () -> facade.getFileWriterAsync("c"));
    }
}

