package com.coroptis.index.log;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

/**
 * Verify basic logging functionality.
 */
public class IntegrationLogTest {

    private Directory directory;
    private final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
    private final TypeDescriptor<String> tds = new TypeDescriptorString();

    @Test
    void test_simple_event_logging() {
        Log<Integer, String> log = Log.<Integer, String>builder()//
                .withDirectory(directory)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .build();
        log.post(3, "aaa");
        log.post(6, "bbb");
        log.post(9, "ccc");
        log.delete(3, tds.getTombstone());
        log.rotate();

        assertEquals(1, directory.getFileNames().count(),
                " 1 file should be created");
        verify_log_data(log);
    }

    @Test
    void test_retated_log_events_are_logged() {
        Log<Integer, String> log = Log.<Integer, String>builder()//
                .withDirectory(directory)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .build();
        log.post(3, "aaa");
        log.post(6, "bbb");
        log.rotate();

        log.post(9, "ccc");
        log.delete(3, tds.getTombstone());
        log.rotate();

        assertEquals(2, directory.getFileNames().count(),
                " 2 file should be created");
        verify_log_data(log);
    }

    private void verify_log_data(final Log<Integer, String> log) {

        assertEquals(4, log.openStreamer().stream().count());
        try (final UnsortedDataFileStreamer<LoggedKey<Integer>, String> streamer = log
                .openStreamer()) {
            final List<Pair<LoggedKey<Integer>, String>> list = streamer
                    .stream().collect(Collectors.toList());
            final Pair<LoggedKey<Integer>, String> p0 = list.get(0);
            assertEquals(LoggedKey.of(LogOperation.POST, 3), p0.getKey());

            final Pair<LoggedKey<Integer>, String> p1 = list.get(1);
            assertEquals(LoggedKey.of(LogOperation.POST, 6), p1.getKey());

            final Pair<LoggedKey<Integer>, String> p2 = list.get(2);
            assertEquals(LoggedKey.of(LogOperation.POST, 9), p2.getKey());

            final Pair<LoggedKey<Integer>, String> p3 = list.get(3);
            assertEquals(LoggedKey.of(LogOperation.DELETE, 3), p3.getKey());
        }
    }

    @BeforeEach
    public void prepareData() {
        directory = new MemDirectory();
    }
}
