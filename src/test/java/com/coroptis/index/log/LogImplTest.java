package com.coroptis.index.log;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

@ExtendWith(MockitoExtension.class)
public class LogImplTest {

    private final List<String> data = List.of("wal-00012.log", "wal-00044.log");

    @Mock
    private LogWriter<String, String> logWriter;

    @Mock
    private LogFileNamesManager logFileNamesManager;

    @Mock
    private LogFilesManager<String, String> logFilesManager;

    private LogImpl<String, String> log;

    @BeforeEach
    void setUp() {
        log = new LogImpl<>(logWriter, logFileNamesManager,
                logFilesManager);
    }

    @Test
    void test_openStreamer() {
        when(logFileNamesManager.getSortedLogFiles()).thenReturn(data);
        final UnsortedDataFileStreamer<LoggedKey<String>, String> streamer = log
                .openStreamer();

        assertNotNull(streamer);
    }

    @Test
    void test_rotate() {
        log.rotate();
        verify(logWriter).rotate();
    }

    @Test
    void test_post() {
        log.post("key1", "value1");
        verify(logWriter).post("key1", "value1");
    }

    @Test
    void test_delete() {
        log.delete("key1", "value1");
        verify(logWriter).delete("key1", "value1");
    }

    @Test
    void test_close() {
        log.close();
        verify(logWriter).close();
    }
}
