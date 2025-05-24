package com.hestiastore.index.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.hestiastore.index.datatype.TypeDescriptor;
import com.hestiastore.index.directory.FileReader;

@ExtendWith(MockitoExtension.class)
public class TypeDescriptorLogOperationTest {

    @Mock
    private FileReader fileReader;

    @Test
    public void test_read_null() throws Exception {
        final TypeDescriptor<LogOperation> tdlk = new TypeDescriptorLogOperation();
        when(fileReader.read()).thenReturn(-1);
        final LogOperation k = tdlk.getTypeReader().read(fileReader);

        assertEquals(null, k);
    }

}
