package org.hestiastore.index.sst;

import static org.mockito.Mockito.when;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.log.Log;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class IndexInternalSynchronizedTest {

    private final static TypeDescriptor<Integer> TD_INTEGER = new TypeDescriptorInteger();
    private TypeDescriptor<String> TD_STRING = new TypeDescriptorString();

    private Directory directory = new MemDirectory();

    @Mock
    private IndexConfiguration<Integer, String> conf;

    @Mock
    private Log<Integer, String> log;

    @Test
    public void test_constructor() throws Exception {
        when(conf.getMaxNumberOfSegmentsInCache()).thenReturn(1000);
        try (IndexInternalSynchronized<Integer, String> synchIndex = new IndexInternalSynchronized<>(
                directory, TD_INTEGER, TD_STRING, conf, log)) {
        }
    }

}
