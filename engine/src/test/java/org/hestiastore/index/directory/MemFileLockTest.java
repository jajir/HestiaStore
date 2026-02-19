package org.hestiastore.index.directory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MemFileLockTest {

    private static final String LOCK_FILE_NAME = ".lock";
    private Directory directory = null;

    @Test
    void test_basicFlow() {
        final FileLock lock = directory.getLock(LOCK_FILE_NAME);

        assertNotNull(lock);

        assertFalse(lock.isLocked());

        lock.lock();

        assertTrue(lock.isLocked());

        lock.unlock();

        assertFalse(lock.isLocked());
    }

    @Test
    void test_lock_again_file() {
        final FileLock lock1 = directory.getLock(LOCK_FILE_NAME);
        assertFalse(lock1.isLocked());
        lock1.lock();
        assertThrows(IllegalStateException.class, () -> lock1.lock());
    }

    @Test
    void test_unlock_unlocked_lock() {
        final FileLock lock1 = directory.getLock(LOCK_FILE_NAME);
        assertFalse(lock1.isLocked());
        lock1.lock();
        assertTrue(lock1.isLocked());
        lock1.unlock();
        assertFalse(lock1.isLocked());
        assertThrows(IllegalStateException.class, () -> lock1.unlock());
    }

    @Test
    void test_lock_locked_file() {
        final FileLock lock1 = directory.getLock(LOCK_FILE_NAME);
        assertFalse(lock1.isLocked());
        lock1.lock();
        assertTrue(lock1.isLocked());

        final FileLock lock2 = directory.getLock(LOCK_FILE_NAME);
        assertTrue(lock2.isLocked());
        assertThrows(IllegalStateException.class, () -> lock2.lock());
    }

    @Test
    void test_stale_lock_is_recovered_when_owner_pid_is_missing() {
        final FileLockMetadata current = FileLockMetadata.currentProcess();
        new FileLockMetadata(Long.MAX_VALUE,
                current.getProcessStartEpochMillis(), current.getHost(),
                "stale-session").write(directory, LOCK_FILE_NAME);
        final FileLock lock = directory.getLock(LOCK_FILE_NAME);

        assertFalse(lock.isLocked());
        assertFalse(directory.isFileExists(LOCK_FILE_NAME));
    }

    @Test
    void test_lock_with_same_pid_but_different_start_is_recovered() {
        final FileLockMetadata current = FileLockMetadata.currentProcess();
        Assumptions.assumeTrue(current.getProcessStartEpochMillis() > 0);
        new FileLockMetadata(current.getPid(),
                current.getProcessStartEpochMillis() + 1L,
                current.getHost(), "stale-session").write(directory,
                        LOCK_FILE_NAME);
        final FileLock lock = directory.getLock(LOCK_FILE_NAME);

        assertFalse(lock.isLocked());
        assertFalse(directory.isFileExists(LOCK_FILE_NAME));
    }

    @Test
    void test_lock_from_other_host_is_not_auto_recovered() {
        final FileLockMetadata current = FileLockMetadata.currentProcess();
        new FileLockMetadata(Long.MAX_VALUE,
                current.getProcessStartEpochMillis(), "other-host-name",
                "remote-session").write(directory, LOCK_FILE_NAME);
        final FileLock lock = directory.getLock(LOCK_FILE_NAME);

        assertTrue(lock.isLocked());
        assertTrue(directory.isFileExists(LOCK_FILE_NAME));
    }

    @Test
    void test_legacy_lock_file_without_metadata_stays_locked() {
        directory.touch(LOCK_FILE_NAME);
        final FileLock lock = directory.getLock(LOCK_FILE_NAME);

        assertTrue(lock.isLocked());
        assertThrows(IllegalStateException.class, () -> lock.lock());
    }

    @BeforeEach
    void createNewStack() {
        directory = new MemDirectory();
    }

}
