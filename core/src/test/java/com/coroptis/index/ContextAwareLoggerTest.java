package com.coroptis.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.segment.Segment;

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

    @Test
    void test_log_debug() {
        when(loggingContext.formatPrefix()).thenReturn("");
        final ContextAwareLogger logger = new ContextAwareLogger(Segment.class,
                loggingContext);

        logger.debug("It's '{}'", 4);
        logger.debug("It's '{}'", 42);
        assertTrue(true);
    }

    @Test
    void test_log_allLevels() {
        when(loggingContext.formatPrefix()).thenReturn("");
        final ContextAwareLogger logger = new ContextAwareLogger(Segment.class,
                loggingContext);

        logger.trace("It's '{}'", "trace");
        logger.debug("It's '{}'", "debug");
        logger.info("It's '{}'", "info");
        logger.warn("It's '{}'", "warn");
        logger.error("It's '{}'", "error");
        assertTrue(true);
    }
}
