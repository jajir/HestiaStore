package org.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.log.Log;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexInternalSynchronizedTest {

    private static final TypeDescriptor<Integer> TD_INTEGER = new TypeDescriptorInteger();
    private static final TypeDescriptor<String> TD_STRING = new TypeDescriptorShortString();

    private Directory directory = new MemDirectory();

    @Mock
    private IndexConfiguration<Integer, String> conf;

    @Mock
    private Log<Integer, String> log;

    @Test
    void test_constructor() {
        when(conf.getMaxNumberOfSegmentsInCache()).thenReturn(1000);
        try (IndexInternalSynchronized<Integer, String> synchIndex = new IndexInternalSynchronized<>(
                directory, TD_INTEGER, TD_STRING, conf, log)) {
            // Intentionally left empty to test constructor
        }

        // If no exception is thrown, then constructor works as expected
        assertTrue(true);
    }

}
