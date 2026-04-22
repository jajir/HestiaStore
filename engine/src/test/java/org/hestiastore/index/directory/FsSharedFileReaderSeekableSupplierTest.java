package org.hestiastore.index.directory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FsSharedFileReaderSeekableSupplierTest {

    @TempDir
    private File tempDir;

    @Test
    void supplier_provides_independent_cursors_over_shared_channel() {
        final Directory directory = new FsDirectory(tempDir);
        final byte[] data = "abcdefghij".getBytes(StandardCharsets.UTF_8);
        try (FileWriter writer = directory.getFileWriter("index.sst")) {
            writer.write(data);
        }

        final FileReaderSeekableSupplier supplier = directory
                .getFileReaderSeekableSupplier("index.sst");
        try (FileReaderSeekable first = supplier.get();
                FileReaderSeekable second = supplier.get()) {
            first.seek(0L);
            second.seek(5L);

            final byte[] firstRead = new byte[3];
            final byte[] secondRead = new byte[3];
            assertEquals(3, first.read(firstRead));
            assertEquals(3, second.read(secondRead));
            assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), firstRead);
            assertArrayEquals("fgh".getBytes(StandardCharsets.UTF_8),
                    secondRead);

            final byte[] firstNext = new byte[2];
            assertEquals(2, first.read(firstNext));
            assertArrayEquals("de".getBytes(StandardCharsets.UTF_8), firstNext);
        }
        supplier.close();
    }

    @Test
    void supplier_rejects_get_after_close() {
        final Directory directory = new FsDirectory(tempDir);
        try (FileWriter writer = directory.getFileWriter("index.sst")) {
            writer.write(new byte[] { 1, 2, 3 });
        }
        final FileReaderSeekableSupplier supplier = directory
                .getFileReaderSeekableSupplier("index.sst");

        supplier.close();

        assertThrows(IllegalStateException.class, supplier::get);
    }
}
