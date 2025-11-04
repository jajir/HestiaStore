package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WriteTransactionTest {

    private static final Entry<String, String> ENTRY1 = new Entry<>("key1",
            "value1");
    private static final Entry<String, String> ENTRY2 = new Entry<>("key2",
            "value2");

    @Mock
    private EntryWriter<String, String> entryWriter;

    @Test
    void test_basic_functionality() {
        final WriteTransaction<String, String> testTransaction = spy(
                new WriteTransaction<String, String>() {

                    @Override
                    public EntryWriter<String, String> open() {
                        return entryWriter;
                    }

                    @Override
                    public void commit() {
                        // it's intentionaly empty
                    }

                });

        testTransaction.execute(writer -> {
            writer.write(ENTRY1);
            writer.write(ENTRY2);
        });

        verify(entryWriter).write(ENTRY1);
        verify(entryWriter).write(ENTRY2);
        verify(entryWriter, times(1)).close();
        verify(testTransaction, times(1)).commit();
    }

    @Test
    void test_exception_during_writing() {
        final WriteTransaction<String, String> testTransaction = spy(
                new WriteTransaction<String, String>() {

                    @Override
                    public EntryWriter<String, String> open() {
                        return entryWriter;
                    }

                    @Override
                    public void commit() {
                        // it's intentionaly empty
                    }

                });

        final Exception e = assertThrows(RuntimeException.class,
                () -> testTransaction.execute(writer -> {
                    writer.write(ENTRY1);
                    writer.write(ENTRY2);
                    throw new RuntimeException("My test exception");
                }));

        assertEquals("My test exception", e.getMessage());
        verify(entryWriter).write(ENTRY1);
        verify(entryWriter).write(ENTRY2);
        verify(entryWriter, times(1)).close();
        verify(testTransaction, never()).commit();
    }

    @Test
    void test_exception_during_close() {
        final WriteTransaction<String, String> testTransaction = spy(
                new WriteTransaction<String, String>() {

                    @Override
                    public EntryWriter<String, String> open() {
                        return entryWriter;
                    }

                    @Override
                    public void commit() {
                        // it's intentionaly empty
                    }

                });
        doThrow(new IndexException("Closing exception")).when(entryWriter)
                .close();
        final Exception e = assertThrows(IndexException.class,
                () -> testTransaction.execute(writer -> {
                    writer.write(ENTRY1);
                    writer.write(ENTRY2);
                }));

        assertEquals("Closing exception", e.getMessage());
        verify(entryWriter).write(ENTRY1);
        verify(entryWriter).write(ENTRY2);
        verify(entryWriter, times(1)).close();
        verify(testTransaction, never()).commit();
    }

    @Test
    void test_exception_during_open() {
        final WriteTransaction<String, String> testTransaction = spy(
                new WriteTransaction<String, String>() {

                    @Override
                    public EntryWriter<String, String> open() {
                        throw new IndexException("open writer exception");
                    }

                    @Override
                    public void commit() {
                        // it's intentionaly empty
                    }

                });

        final Exception e = assertThrows(IndexException.class,
                () -> testTransaction.execute(writer -> {
                    writer.write(ENTRY1);
                    writer.write(ENTRY2);
                }));

        assertEquals("open writer exception", e.getMessage());
        verify(entryWriter, never()).write(ENTRY1);
        verify(entryWriter, never()).write(ENTRY2);
        verify(entryWriter, never()).close();
        verify(testTransaction, never()).commit();
    }

}
