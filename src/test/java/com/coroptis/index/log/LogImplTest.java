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
public class LogImplTest {

    private Directory directory;
    private final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
    private final TypeDescriptor<String> tds = new TypeDescriptorString();

    @Test
    void test_simple_event_logging() {
        Log<Integer, String> log = new LogImpl<>(directory, "log", tdi,
                tds.getTypeWriter(), tds.getTypeReader());
        LogWriter<Integer, String> logWriter = log.openWriter();
        logWriter.post(3, "aaa");
        logWriter.post(6, "bbb");
        logWriter.post(9, "ccc");
        logWriter.delete(3, tds.getTombstone());
        logWriter.close();

        verify_log_data(log);
    }

    @Test
    void test_splitted_event_logging() {
        Log<Integer, String> log = new LogImpl<>(directory, "log", tdi,
                tds.getTypeWriter(), tds.getTypeReader());
        final LogWriter<Integer, String> logWriter1 = log.openWriter();
        logWriter1.post(3, "aaa");
        logWriter1.post(6, "bbb");
        logWriter1.close();

        final LogWriter<Integer, String> logWriter2 = log.openWriter();
        logWriter2.post(9, "ccc");
        logWriter2.delete(3, tds.getTombstone());
        logWriter2.close();

        verify_log_data(log);
    }

    private void verify_log_data(final Log<Integer, String> log) {
        assertEquals(1, directory.getFileNames().count());

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
