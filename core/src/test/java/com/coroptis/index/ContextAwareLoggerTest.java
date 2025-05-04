package com.coroptis.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ContextAwareLoggerTest {

    @Mock
    private LoggingContext loggingContext;

    @Test
    void test_missing_class() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new ContextAwareLogger(null, loggingContext));

        assertEquals("Class cannot be null", e.getMessage());
    }

    @Test
    void test_missing_logging_context() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new ContextAwareLogger(PairIteratorWithLockTest.class,
                        null));

        assertEquals("LoggingContext cannot be null", e.getMessage());
    }
}
