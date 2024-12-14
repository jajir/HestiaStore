package com.coroptis.index.log;

import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.PairWriter;

@ExtendWith(MockitoExtension.class)
public class LogUnsortedFileWriterImplTest {

    @Mock
    private PairWriter<LoggedKey<String>, Integer> writer;

    @Test
    void test_post() {
        try (final LogUnsortedFileWriterImpl<String, Integer> logWriter = new LogUnsortedFileWriterImpl<>(
                writer)) {
            logWriter.post("kachna", 752);
            verify(writer).put(LoggedKey.of(LogOperation.POST, "kachna"), 752);
        }
    }

    @Test
    void test_delete() {
        try (final LogUnsortedFileWriterImpl<String, Integer> logWriter = new LogUnsortedFileWriterImpl<>(
                writer)) {
            logWriter.delete("kachna", 7527);
            verify(writer).put(LoggedKey.of(LogOperation.DELETE, "kachna"),
                    7527);
        }
    }

    @Test
    void test_close() {
        try (final LogUnsortedFileWriterImpl<String, Integer> logWriter = new LogUnsortedFileWriterImpl<>(
                writer)) {
        }
        verify(writer).close();
    }

}
