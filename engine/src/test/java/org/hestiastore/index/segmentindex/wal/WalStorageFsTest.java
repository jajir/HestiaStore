package org.hestiastore.index.segmentindex.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalStorageFsTest {

    @TempDir
    Path tempDir;

    @Test
    void listFileNamesReturnsConsumableStreamAfterDirectoryListingCloses() {
        final WalStorageFs storage = new WalStorageFs(tempDir);
        storage.append("000000000001.wal", "a".getBytes(StandardCharsets.UTF_8),
                0, 1);
        storage.append("000000000003.wal", "b".getBytes(StandardCharsets.UTF_8),
                0, 1);
        storage.append("000000000002.wal", "c".getBytes(StandardCharsets.UTF_8),
                0, 1);

        try (var names = storage.listFileNames()) {
            assertEquals(List.of("000000000001.wal", "000000000002.wal",
                    "000000000003.wal"), names.toList());
        }
    }
}
