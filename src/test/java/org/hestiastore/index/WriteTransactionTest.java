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
                        // it's intentionaly empty
                    }

                });

        testTransaction.execute(writer -> {
            writer.write(PAIR1);
            writer.write(PAIR2);
        });

        verify(pairWriter).write(PAIR1);
        verify(pairWriter).write(PAIR2);
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
                        // it's intentionaly empty
                    }

                });

        final Exception e = assertThrows(RuntimeException.class,
                () -> testTransaction.execute(writer -> {
                    writer.write(PAIR1);
                    writer.write(PAIR2);
                    throw new RuntimeException("My test exception");
                }));

        assertEquals("My test exception", e.getMessage());
        verify(pairWriter).write(PAIR1);
        verify(pairWriter).write(PAIR2);
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
                        // it's intentionaly empty
                    }

                });
        doThrow(new IndexException("Closing exception")).when(pairWriter)
                .close();
        final Exception e = assertThrows(IndexException.class,
                () -> testTransaction.execute(writer -> {
                    writer.write(PAIR1);
                    writer.write(PAIR2);
                }));

        assertEquals("Closing exception", e.getMessage());
        verify(pairWriter).write(PAIR1);
        verify(pairWriter).write(PAIR2);
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
                        // it's intentionaly empty
                    }

                });

        final Exception e = assertThrows(IndexException.class,
                () -> testTransaction.execute(writer -> {
                    writer.write(PAIR1);
                    writer.write(PAIR2);
                }));

        assertEquals("open writer exception", e.getMessage());
        verify(pairWriter, never()).write(PAIR1);
        verify(pairWriter, never()).write(PAIR2);
        verify(pairWriter, never()).close();
        verify(testTransaction, never()).commit();
    }

}
