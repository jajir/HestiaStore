package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SegmentDataFactoryImplTest {

    @Mock
    private SegmentDataSupplier<Integer, String> supplier;

    @Test
    void test_getSegmentData() throws Exception {
        final SegmentDataFactoryImpl<Integer, String> factory = new SegmentDataFactoryImpl<>(
                supplier);
        final SegmentData<Integer, String> result = factory.getSegmentData();

        assertNotNull(result);
        assertTrue(result instanceof SegmentDataLazyLoaded);
    }

    @Test
    void test_supplier_is_null() throws Exception {
        final Exception e = assertThrows(NullPointerException.class,
                () -> new SegmentDataFactoryImpl<>(null));

        assertEquals("segmentDataSupplier cannot be null", e.getMessage());
    }

}
