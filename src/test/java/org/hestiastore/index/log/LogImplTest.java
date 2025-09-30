package org.hestiastore.index.log;

import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LogImplTest {

    @Mock
    private LogWriter<String, String> logWriter;

    @Mock
    private LogFileNamesManager logFileNamesManager;

    @Mock
    private LogFilesManager<String, String> logFilesManager;

    private LogImpl<String, String> log;

    @BeforeEach
    void setUp() {
        log = new LogImpl<>(logWriter);
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
