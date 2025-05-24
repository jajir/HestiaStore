package com.hestiastore.index.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.hestiastore.index.directory.Directory;

@ExtendWith(MockitoExtension.class)
public class LogFileNamesManagerTest {

    @Mock
    private Directory directory;

    private final List<String> data = List.of("wal-00012.log", "ochechule.txt",
            "wal-00044.log", "wal-00002.log", "wal-00003.log");

    @Test
    void test_getNewLogFileName_file_list() {
        when(directory.getFileNames()).thenReturn(data.stream());
        final LogFileNamesManager manager = new LogFileNamesManager(directory);

        assertEquals("wal-00045.log", manager.getNewLogFileName());
    }

    @Test
    void test_getNewLogFileName_max_number_of_logFiles() {
        when(directory.getFileNames())
                .thenReturn(List.of("wal-99999.log").stream());
        final LogFileNamesManager manager = new LogFileNamesManager(directory);

        final Exception e = assertThrows(IllegalStateException.class,
                () -> manager.getNewLogFileName());

        assertEquals("Max number of log files reached", e.getMessage());
    }

    @Test
    void test_getNewLogFileName_firstFile() {
        when(directory.getFileNames())
                .thenReturn(new ArrayList<String>().stream());
        final LogFileNamesManager manager = new LogFileNamesManager(directory);

        assertEquals("wal-00000.log", manager.getNewLogFileName());
    }

}
