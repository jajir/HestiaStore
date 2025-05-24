package com.hestiastore.index.directory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class PropsTest {

    private final MemDirectory directory = new MemDirectory();

    @Test
    void test_storing_data() throws Exception {
        final Props props = new Props(directory, "pok1.properties");
        props.setInt("test0", 21);
        props.setLong("test1", 42);
        assertEquals(21, props.getInt("test0"));
        assertEquals(42, props.getLong("test1"));
    }

    @Test
    void test_read_default_values() throws Exception {
        final Props props = new Props(directory, "pok1.properties");
        assertEquals(0, props.getInt("test0"));
        assertEquals(0, props.getLong("test1"));
    }

    @Test
    void test_write_and_read() throws Exception {
        final Props props1 = new Props(directory, "pok1.properties");
        props1.setInt("test0", 21);
        props1.setLong("test1", 42);
        props1.writeData();

        final Props props2 = new Props(directory, "pok1.properties");
        assertEquals(21, props2.getInt("test0"));
        assertEquals(42, props2.getLong("test1"));
    }

    private final static String LONG_KEY1 = "very_long_key_with_some_additional_data1";
    private final static String LONG_KEY2 = "very_long_key_with_some_additional_data2_tst";

    @Test
    public void test_write_and_read_long_keys() throws Exception {
        final Props props1 = new Props(directory, "pok1.properties");
        props1.setInt(LONG_KEY1, 21);
        props1.setLong(LONG_KEY2, 42);
        props1.writeData();

        final Props props2 = new Props(directory, "pok1.properties");
        assertEquals(21, props2.getInt(LONG_KEY1));
        assertEquals(42, props2.getLong(LONG_KEY2));
    }

}
