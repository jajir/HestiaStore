package org.hestiastore.index.properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DirectoryPropertyStoreTest {

    private static final String FILE_NAME = "store.properties";

    private MemDirectory directory;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
    }

    @Test
    void transaction_persists_changes() {
        final PropertyStoreimpl store = new PropertyStoreimpl(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(directory),
                FILE_NAME, false);

        final PropertyTransaction tx = store.beginTransaction();
        tx.openPropertyWriter().setInt("alpha", 7).setLong("beta", 42L)
                .setBoolean("gamma", true);
        tx.close();

        // Reload to ensure values are persisted on disk
        final PropertyStoreimpl reloaded = new PropertyStoreimpl(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(directory),
                FILE_NAME, true);
        final PropertyView reloadedView = reloaded.snapshot();
        assertEquals(7, reloadedView.getInt("alpha"));
        assertEquals(42L, reloadedView.getLong("beta"));
        assertEquals(true, reloadedView.getBoolean("gamma"));
    }

    @Test
    void transaction_persists_changes_on_close() {
        final PropertyStoreimpl store = new PropertyStoreimpl(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(directory),
                FILE_NAME, false);
        final PropertyTransaction tx = store.beginTransaction();
        tx.openPropertyWriter().setInt("alpha", 9);
        tx.close();
        final PropertyStoreimpl reloaded = new PropertyStoreimpl(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(directory),
                FILE_NAME, true);
        assertEquals(9, reloaded.snapshot().getInt("alpha"));
    }

    @Test
    void getters_return_zero_when_missing() {
        final PropertyStoreimpl store = new PropertyStoreimpl(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(directory),
                FILE_NAME, false);
        final PropertyView view = store.snapshot();
        assertEquals(0, view.getInt("missing-int"));
        assertEquals(0L, view.getLong("missing-long"));
        assertEquals(0D, view.getDouble("missing-double"));
        assertEquals(false, view.getBoolean("missing-bool"));
    }

    @Test
    void snapshot_returns_read_only_copy() {
        final PropertyStoreimpl store = new PropertyStoreimpl(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(directory),
                FILE_NAME, false);
        final PropertyTransaction tx = store.beginTransaction();
        tx.openPropertyWriter().setInt("foo", 99).setBoolean("bar", true);
        tx.close();

        final PropertyView view = store.snapshot();
        assertNotNull(view);
        assertEquals(99, view.getInt("foo"));
        assertEquals(true, view.getBoolean("bar"));
    }
}
