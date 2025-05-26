package org.hestiastore.index.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.log.LogOperation;
import org.hestiastore.index.log.TypeDescriptorLogOperation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
