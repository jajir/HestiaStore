package org.hestiastore.index.segmentindex.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalPathStorageTest {

    @TempDir
    Path tempDir;

    @Test
    void listFileNamesReturnsConsumableStreamAfterDirectoryListingCloses() {
        try (WalPathStorage storage = new WalPathStorage(tempDir)) {
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

    @Test
    void appendChannelSurvivesSyncAndMutatingOperations() {
        try (WalPathStorage storage = new WalPathStorage(tempDir)) {
            storage.append("active.wal", bytes("a"), 0, 1);
            storage.sync("active.wal");
            storage.append("active.wal", bytes("b"), 0, 1);
            assertEquals("ab", readString(storage, "active.wal"));

            storage.overwrite("active.wal", bytes("c"), 0, 1);
            storage.append("active.wal", bytes("d"), 0, 1);
            assertEquals("cd", readString(storage, "active.wal"));

            storage.truncate("active.wal", 1L);
            storage.append("active.wal", bytes("e"), 0, 1);
            assertEquals("ce", readString(storage, "active.wal"));

            storage.rename("active.wal", "renamed.wal");
            storage.append("renamed.wal", bytes("f"), 0, 1);
            assertEquals("cef", readString(storage, "renamed.wal"));

            storage.delete("renamed.wal");
            assertFalse(Files.exists(tempDir.resolve("renamed.wal")));
        }
    }

    private static byte[] bytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String readString(final WalPathStorage storage,
            final String fileName) {
        return new String(storage.readAll(fileName), StandardCharsets.UTF_8);
    }
}
