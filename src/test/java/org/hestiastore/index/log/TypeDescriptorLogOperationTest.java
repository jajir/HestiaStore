package org.hestiastore.index.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.FileReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TypeDescriptorLogOperationTest {

    @Mock
    private FileReader fileReader;

    @Test
    void test_read_null() {
        final TypeDescriptor<LogOperation> tdlk = new TypeDescriptorLogOperation();
        when(fileReader.read()).thenReturn(-1);
        final LogOperation k = tdlk.getTypeReader().read(fileReader);

        assertEquals(null, k);
    }

}
