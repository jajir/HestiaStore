package com.coroptis.index;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FormatTest {

    private final Logger logger = LoggerFactory.getLogger(FormatTest.class);

    @Test
    void test_basic_fomatting() throws Exception {
        DecimalFormat df = (DecimalFormat) NumberFormat
                .getNumberInstance(Locale.getDefault());
        logger.debug(Locale.getDefault().toString());
        logger.debug(df.format(1_123_456));

        assertTrue(F.fmt(1_123_456).contains("123"));
        assertTrue(F.fmt(1_123_456).contains("456"));
    }

}
