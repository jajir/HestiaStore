package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.File;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.DirectoryFacade;
import org.hestiastore.index.directory.FsDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Reproduces a real-life failure where SortedDataFileWriterTx commits by
 * renaming a non-existent temporary file (".tmp") because open() writes
 * into the final file name instead of the temp one.
 *
 * Expected: commit should succeed (writer writes to temp and commit renames
 * temp→final). Actual (current code): commit throws IndexException because
 * "<final>.tmp" does not exist on disk.
 */
class SortedDataFileWriterTxTest {

    @TempDir
    File tempDir;

    @Test
    void commit_should_not_fail_due_to_missing_tmp_file() {
        final Directory dir = new FsDirectory(tempDir);

        final String fileName = "segment-00000-delta-000.cache";
        final int bufferSize = 1024;
        final TypeDescriptorShortString td = new TypeDescriptorShortString();

        final SortedDataFileWriterTx<String, String> tx = new SortedDataFileWriterTx<>(
                fileName, DirectoryFacade.of(dir), bufferSize, td, td);

        try (final EntryWriter<String, String> w = tx.open()) {
            w.write(Entry.of("K", "V"));
        }

        // This currently fails because tmp file was never created.
        // The desired behavior is that commit succeeds.
        assertDoesNotThrow(tx::commit);
    }
}
