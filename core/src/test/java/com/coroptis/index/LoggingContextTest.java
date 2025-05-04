package com.coroptis.index;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class LoggingContextTest {

    @Test
    void test_prefix() {
        LoggingContext context = new LoggingContext("testIndex");

        assertEquals("index 'testIndex': ", context.formatPrefix());
    }

}
