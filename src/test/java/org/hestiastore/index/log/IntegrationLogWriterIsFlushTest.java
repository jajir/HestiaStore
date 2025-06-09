package org.hestiastore.index.log;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.hestiastore.index.Pair;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test verify that logged data are immeditelly stored to drive.
 */
class IntegrationLogWriterIsFlushTest {

    private final TypeDescriptor<Long> tdl = new TypeDescriptorLong();
    private final TypeDescriptor<String> tds = new TypeDescriptorString();

    private Directory directory;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
    }

    @Test
    void test_writting_to_log() {
        final Log<Long, String> log = Log.<Long, String>builder()//
                .withDirectory(directory)//
                .withKeyTypeDescriptor(tdl)//
                .withValueTypeDescriptor(tds)//
                .build();

        log.post(1L, "aaa");
        log.post(2L, "bbb");
        log.post(3L, "ccc");
        log.rotate();

        final List<Pair<LoggedKey<Long>, String>> pairs = log.openStreamer()
                .stream().toList();
        assertEquals(3, pairs.size());

        Pair<LoggedKey<Long>, String> pair1 = pairs.get(0);
        Pair<LoggedKey<Long>, String> pair2 = pairs.get(1);
        Pair<LoggedKey<Long>, String> pair3 = pairs.get(2);
        assertEquals(1L, pair1.getKey().getKey());
        assertEquals(2L, pair2.getKey().getKey());
        assertEquals(3L, pair3.getKey().getKey());
        assertEquals("aaa", pair1.getValue());
        assertEquals("bbb", pair2.getValue());
        assertEquals("ccc", pair3.getValue());
        log.close();
    }

}
