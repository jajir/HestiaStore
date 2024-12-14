package com.coroptis.index.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.CloseablePairReader;
import com.coroptis.index.Pair;

@ExtendWith(MockitoExtension.class)
public class LogFilesSpliteratorTest {

    // This verify that FILE_NAMES are could be immutable object
    private final List<String> FILE_NAMES = List.of("log1", "log2");
    private final Pair<LoggedKey<String>, String> PAIR1 = new Pair<>(
            LoggedKey.of(LogOperation.POST, "key1"), "value1");
    private final Pair<LoggedKey<String>, String> PAIR2 = new Pair<>(
            LoggedKey.of(LogOperation.POST, "key2"), "value2");
    private final Pair<LoggedKey<String>, String> PAIR3 = new Pair<>(
            LoggedKey.of(LogOperation.DELETE, "key1"), "value3");

    @Mock
    private LogFilesManager<String, String> logFilesManager;

    @Mock
    private CloseablePairReader<LoggedKey<String>, String> pairReader1;

    @Mock
    private CloseablePairReader<LoggedKey<String>, String> pairReader2;

    @SuppressWarnings("unchecked")
    @Test
    void test_tryAdvance_withData() {
        try (final LogFilesSpliterator<String, String> spliterator = new LogFilesSpliterator<>(
                logFilesManager, FILE_NAMES)) {
            when(logFilesManager.openReader("log1")).thenReturn(pairReader1);
            when(pairReader1.read()).thenReturn(PAIR1, PAIR2,
                    (Pair<LoggedKey<String>, String>) null);
            when(logFilesManager.openReader("log2")).thenReturn(pairReader2);
            when(pairReader2.read()).thenReturn(PAIR3,
                    (Pair<LoggedKey<String>, String>) null);

            assertTrue(
                    spliterator.tryAdvance(pair -> assertEquals(PAIR1, pair)));
            assertTrue(
                    spliterator.tryAdvance(pair -> assertEquals(PAIR2, pair)));
            assertTrue(
                    spliterator.tryAdvance(pair -> assertEquals(PAIR3, pair)));
            assertFalse(spliterator.tryAdvance(pair -> {
            }));
        }
    }

    @Test
    void test_tryAdvance_noData() {
        try (final LogFilesSpliterator<String, String> spliterator = new LogFilesSpliterator<>(
                logFilesManager, List.of())) {
            assertFalse(spliterator.tryAdvance(pair -> {
            }));
        }
    }

}
