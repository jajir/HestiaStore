package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class WriteTransactionTest {

    private static final Pair<String, String> PAIR1 = new Pair<>("key1",
            "value1");
    private static final Pair<String, String> PAIR2 = new Pair<>("key2",
            "value2");

    @Mock
    private PairWriter<String, String> pairWriter;

    @Test
    void test_basic_functionality() {
        final WriteTransaction<String, String> testTransaction = spy(
                new WriteTransaction<String, String>() {

                    @Override
                    public PairWriter<String, String> openWriter() {
                        return pairWriter;
                    }

                    @Override
                    public void commit() {
                    }

                });

        testTransaction.execute(writer -> {
            writer.put(PAIR1);
            writer.put(PAIR2);
        });

        verify(pairWriter).put(PAIR1);
        verify(pairWriter).put(PAIR2);
        verify(pairWriter, times(1)).close();
        verify(testTransaction, times(1)).commit();
    }

    @Test
    void test_exception_during_writing() {
        final WriteTransaction<String, String> testTransaction = spy(
                new WriteTransaction<String, String>() {

                    @Override
                    public PairWriter<String, String> openWriter() {
                        return pairWriter;
                    }

                    @Override
                    public void commit() {
                    }

                });

        final Exception e = assertThrows(IndexException.class,
                () -> testTransaction.execute(writer -> {
                    writer.put(PAIR1);
                    writer.put(PAIR2);
                    throw new IOException("My test exception");
                }));

        assertEquals("Error during writing pairs: My test exception",
                e.getMessage());
        verify(pairWriter).put(PAIR1);
        verify(pairWriter).put(PAIR2);
        verify(pairWriter, times(1)).close();
        verify(testTransaction, never()).commit();
    }

    @Test
    void test_exception_during_close() {
        final WriteTransaction<String, String> testTransaction = spy(
                new WriteTransaction<String, String>() {

                    @Override
                    public PairWriter<String, String> openWriter() {
                        return pairWriter;
                    }

                    @Override
                    public void commit() {
                    }

                });
        doThrow(new IndexException("Closing exception")).when(pairWriter)
                .close();
        final Exception e = assertThrows(IndexException.class,
                () -> testTransaction.execute(writer -> {
                    writer.put(PAIR1);
                    writer.put(PAIR2);
                }));

        assertEquals("Error during writing pairs: Closing exception",
                e.getMessage());
        verify(pairWriter).put(PAIR1);
        verify(pairWriter).put(PAIR2);
        verify(pairWriter, times(1)).close();
        verify(testTransaction, never()).commit();
    }

    @Test
    void test_exception_during_openWriter() {
        final WriteTransaction<String, String> testTransaction = spy(
                new WriteTransaction<String, String>() {

                    @Override
                    public PairWriter<String, String> openWriter() {
                        throw new IndexException("open writer exception");
                    }

                    @Override
                    public void commit() {
                    }

                });

        final Exception e = assertThrows(IndexException.class,
                () -> testTransaction.execute(writer -> {
                    writer.put(PAIR1);
                    writer.put(PAIR2);
                }));

        assertEquals("Error during writing pairs: open writer exception",
                e.getMessage());
        verify(pairWriter, never()).put(PAIR1);
        verify(pairWriter, never()).put(PAIR2);
        verify(pairWriter, never()).close();
        verify(testTransaction, never()).commit();
    }

}
