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
    void test_log_constructor() {
        Log<Integer, String> log = new LogImpl<>(directory, "log", tdi,
                tds.getTypeWriter(), tds.getTypeReader());
        LogWriter<Integer, String> logWriter = log.openWriter();
        logWriter.post(3, "aaa");
        logWriter.post(6, "bbb");
        logWriter.post(9, "ccc");
        logWriter.delete(3, tds.getTombstone());
        logWriter.close();

        assertEquals(1, directory.getFileNames().count());

        assertEquals(4, log.openStreamer().stream().count());
        try (final UnsortedDataFileStreamer<LoggedKey<Integer>, String> streamer = log
                .openStreamer()) {
            final List<Pair<LoggedKey<Integer>, String>> list = streamer
                    .stream().collect(Collectors.toList());
            final Pair<LoggedKey<Integer>, String> p1 = list.get(0);
            assertEquals(LoggedKey.of(LogOperation.POST, 3), p1.getKey());
        }
    }

    @BeforeEach
    public void prepareData() {
        directory = new MemDirectory();
    }
}
