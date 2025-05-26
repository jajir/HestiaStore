package org.hestiastore.index.log;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.log.Log;
import org.junit.jupiter.api.Test;

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
