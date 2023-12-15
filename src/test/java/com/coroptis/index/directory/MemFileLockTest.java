package com.coroptis.index.directory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MemFileLockTest {

    private final static String LOCK_FILE_NAME = ".lock";
    private Directory directory = null;

    @Test
    void test_basicFlow() throws Exception {
        final FileLock lock = directory.getLock(LOCK_FILE_NAME);

        assertNotNull(lock);

        assertFalse(lock.isLocked());

        lock.lock();

        assertTrue(lock.isLocked());

        lock.unlock();

        assertFalse(lock.isLocked());
    }

    @Test
    void test_lock_again_file() throws Exception {
        final FileLock lock1 = directory.getLock(LOCK_FILE_NAME);
        assertFalse(lock1.isLocked());
        lock1.lock();
        assertThrows(IllegalStateException.class, () -> lock1.lock());
    }

    @Test
    void test_unlock_unlocked_lock() throws Exception {
        final FileLock lock1 = directory.getLock(LOCK_FILE_NAME);
        assertFalse(lock1.isLocked());
        lock1.lock();
        assertTrue(lock1.isLocked());
        lock1.unlock();
        assertFalse(lock1.isLocked());
        assertThrows(IllegalStateException.class, () -> lock1.unlock());
    }

    @Test
    void test_lock_locked_file() throws Exception {
        final FileLock lock1 = directory.getLock(LOCK_FILE_NAME);
        assertFalse(lock1.isLocked());
        lock1.lock();
        assertTrue(lock1.isLocked());

        final FileLock lock2 = directory.getLock(LOCK_FILE_NAME);
        assertTrue(lock2.isLocked());
        assertThrows(IllegalStateException.class, () -> lock2.lock());
    }

    @BeforeEach
    void createNewStack() {
        directory = new MemDirectory();
    }

}
