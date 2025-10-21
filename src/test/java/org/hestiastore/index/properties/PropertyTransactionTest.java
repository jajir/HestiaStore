package org.hestiastore.index.properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PropertyTransactionTest {

    private Properties target;

    @Mock
    private PropertyStoreimpl store;

    private PropertyTransaction transaction;

    @BeforeEach
    void setUp() {
        target = new Properties();
        target.setProperty("alpha", "1");
        target.setProperty("beta", "2");
        transaction = new PropertyTransaction(store, target);
    }

    @Test
    void writer_changes_are_applied_on_close_and_persisted() {
        final PropertyWriter writer = transaction.openPropertyWriter();

        writer.setInt("alpha", 42).setBoolean("gamma", true);

        // target should remain unaffected until close
        assertEquals("1", target.getProperty("alpha"));
        assertNull(target.getProperty("gamma"));

        transaction.close();

        assertEquals("42", target.getProperty("alpha"));
        assertEquals("2", target.getProperty("beta"));
        assertEquals("true", target.getProperty("gamma"));
        verify(store).writeToDisk(target);
    }

    @Test
    void writer_initialises_with_snapshot_values() {
        target.setProperty("delta", "original");
        transaction = new PropertyTransaction(store, target);

        final PropertyWriter writer = transaction.openPropertyWriter();
        writer.setString("epsilon", "new-value");

        transaction.close();

        assertEquals("original", target.getProperty("delta"));
        assertEquals("new-value", target.getProperty("epsilon"));
        verify(store).writeToDisk(target);
    }

    @Test
    void opening_writer_twice_throws() {
        transaction.openPropertyWriter();
        assertThrows(IllegalStateException.class,
                () -> transaction.openPropertyWriter());
    }
}
