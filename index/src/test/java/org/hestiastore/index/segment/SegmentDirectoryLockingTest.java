package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentDirectoryLockingTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(7);
    private static final SegmentDirectoryLayout LAYOUT = new SegmentDirectoryLayout(
            SEGMENT_ID);

    @TempDir
    private File tempDir;

    @Test
    void tryLock_creates_and_releases_lock_file_in_memory_directory() {
        final Directory directory = new MemDirectory();
        final SegmentDirectoryLocking locking = new SegmentDirectoryLocking(
                directory, LAYOUT);

        assertTrue(locking.tryLock());

        assertTrue(directory.isFileExists(LAYOUT.getLockFileName()));

        locking.unlock();

        assertFalse(directory.isFileExists(LAYOUT.getLockFileName()));
    }

    @Test
    void tryLock_creates_and_releases_lock_file_in_filesystem_directory() {
        final Directory directory = new FsDirectory(tempDir);
        final SegmentDirectoryLocking locking = new SegmentDirectoryLocking(
                directory, LAYOUT);

        assertTrue(locking.tryLock());

        assertTrue(directory.isFileExists(LAYOUT.getLockFileName()));

        locking.unlock();

        assertFalse(directory.isFileExists(LAYOUT.getLockFileName()));
    }

    @Test
    void lock_fails_when_lock_file_exists() {
        final Directory directory = new MemDirectory();
        directory.touch(LAYOUT.getLockFileName());
        final SegmentDirectoryLocking locking = new SegmentDirectoryLocking(
                directory, LAYOUT);

        assertFalse(locking.tryLock());
    }
}
