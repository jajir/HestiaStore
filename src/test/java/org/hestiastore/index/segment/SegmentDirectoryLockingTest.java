package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentDirectoryLockingTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(7);
    private static final SegmentDirectoryLayout LAYOUT = new SegmentDirectoryLayout(
            SEGMENT_ID);

    @TempDir
    private File tempDir;

    @Test
    void lock_creates_and_releases_lock_file_in_memory_directory() {
        final Directory directory = new MemDirectory();
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(directory);
        final SegmentDirectoryLocking locking = new SegmentDirectoryLocking(
                asyncDirectory, LAYOUT);

        locking.lock();

        assertTrue(directory.isFileExists(LAYOUT.getLockFileName()));

        locking.unlock();

        assertFalse(directory.isFileExists(LAYOUT.getLockFileName()));
    }

    @Test
    void lock_creates_and_releases_lock_file_in_filesystem_directory() {
        final Directory directory = new FsDirectory(tempDir);
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(directory);
        final SegmentDirectoryLocking locking = new SegmentDirectoryLocking(
                asyncDirectory, LAYOUT);

        locking.lock();

        assertTrue(directory.isFileExists(LAYOUT.getLockFileName()));

        locking.unlock();

        assertFalse(directory.isFileExists(LAYOUT.getLockFileName()));
    }

    @Test
    void lock_fails_when_lock_file_exists() {
        final Directory directory = new MemDirectory();
        directory.touch(LAYOUT.getLockFileName());
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(directory);
        final SegmentDirectoryLocking locking = new SegmentDirectoryLocking(
                asyncDirectory, LAYOUT);

        final SegmentDirectoryLocking.LockBusyException error = assertThrows(
                SegmentDirectoryLocking.LockBusyException.class, locking::lock);

        assertTrue(error.getMessage() != null
                && error.getMessage().contains("already locked"));
    }
}
