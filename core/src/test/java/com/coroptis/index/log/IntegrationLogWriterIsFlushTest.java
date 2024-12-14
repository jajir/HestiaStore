package com.coroptis.index.log;

import org.junit.jupiter.api.Test;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorLong;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

/**
 * Test verify that logged data are immeditelly stored to drive.
 */
public class IntegrationLogWriterIsFlushTest {

    private final TypeDescriptor<Long> tdl = new TypeDescriptorLong();
    private final TypeDescriptor<String> tds = new TypeDescriptorString();

    private Directory directory = new MemDirectory();

    void setUp() {
        directory = new MemDirectory();
    }

    @Test
    void test_writting_to_log() {
        final Log<Long, String> index = Log.<Long, String>builder()//
                .withDirectory(directory)//
                .withKeyTypeDescriptor(tdl)//
                .withValueTypeDescriptor(tds)//
                .build();

            index.post(1L, "aaa");
            index.post(2L, "bbb");
            index.post(3L, "ccc");
    }

}
