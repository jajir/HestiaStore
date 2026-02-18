package org.hestiastore.index.unsorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.NoSuchElementException;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.directory.FileReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DataFileIteratorTest {

    @Mock
    private TypeReader<String> keyTypeReader;

    @Mock
    private TypeReader<Integer> valueTypeReader;

    @Mock
    private FileReader reader;

    private DataFileIterator<String, Integer> iterator;

    @Test
    void test_simple() {
        when(keyTypeReader.read(reader)).thenReturn("A", "B", "C", null);
        when(valueTypeReader.read(reader)).thenReturn(1, 2, 3, null);
        iterator = new DataFileIterator<>(keyTypeReader, valueTypeReader,
                reader);

        assertTrue(iterator.hasNext());
        assertEquals(Entry.of("A", 1), iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(Entry.of("B", 2), iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(Entry.of("C", 3), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    void test_empty() {
        when(keyTypeReader.read(reader)).thenReturn(null);
        iterator = new DataFileIterator<>(keyTypeReader, valueTypeReader,
                reader);

        assertFalse(iterator.hasNext());
    }

    @Test
    void test_NoSuchElement() {
        when(keyTypeReader.read(reader)).thenReturn(null);
        iterator = new DataFileIterator<>(keyTypeReader, valueTypeReader,
                reader);

        assertFalse(iterator.hasNext());
        final Exception e = assertThrows(NoSuchElementException.class,
                () -> iterator.next());
        assertEquals("No more elements", e.getMessage());
    }

    @Test
    void test_constructor_missing_keyTypeReader() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new DataFileIterator<>(null, valueTypeReader, reader));
        assertEquals("Property 'keyReader' must not be null.", e.getMessage());
    }

    @Test
    void test_constructor_missing_valueTypeReader() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new DataFileIterator<>(keyTypeReader, null, reader));
        assertEquals("Property 'valueReader' must not be null.",
                e.getMessage());
    }

    @Test
    void test_constructor_missing_reader() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new DataFileIterator<>(keyTypeReader, valueTypeReader,
                        null));
        assertEquals("Property 'reader' must not be null.", e.getMessage());
    }

    @AfterEach
    void tearDown() {
        iterator = null;
    }

}
