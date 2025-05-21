package com.coroptis.index.sst;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.log.Log;

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
        try (IndexInternalSynchronized<Integer, String> synchIndex = new IndexInternalSynchronized<>(
                directory, TD_INTEGER, TD_STRING, conf, log)) {
        }
    }

}
