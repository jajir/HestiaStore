package com.coroptis.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class FileNameUtilTest {

    @Test
    public void test_getPaddedId() {
        assertEquals("00003", FileNameUtil.getPaddedId(3, 5));
        assertEquals("100", FileNameUtil.getPaddedId(100, 3));
        assertEquals("999", FileNameUtil.getPaddedId(999, 3));
    }

    @Test
    public void test_getPaddedId_id_isTooBig() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> FileNameUtil.getPaddedId(100000, 3));
        assertEquals("Id '100000' is too long to be padded to '3' positions.",
                e.getMessage());
    }

    @Test
    public void test_getPaddedId_id_isNegative() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> FileNameUtil.getPaddedId(-100, 3));
        assertEquals("Id '-100' is negative.",
                e.getMessage());
    }

    @Test
    public void test_getPaddedId_length_isNegative() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> FileNameUtil.getPaddedId(100, -3));
        assertEquals("Length '-3' is negative.",
                e.getMessage());
    }

}
