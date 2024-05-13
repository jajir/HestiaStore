package com.coroptis.index.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.scarceindex.ScarceIndexCache;

public class LoggedKeyStoreTest {

    private final static TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();

    @Test
    public void test_simple_read_write() throws Exception {
        final TypeDescriptorLoggedKey<Integer> tdlk = new TypeDescriptorLoggedKey<>(tdi);

        final LoggedKey<Integer> k1 = tdlk.getConvertorFromBytes()
                .fromBytes(tdlk.getConvertorToBytes().toBytes(LoggedKey.<Integer>of(LogOperation.POST, 87)));
        assertEquals(87, k1.getKey());
        assertEquals(LogOperation.POST, k1.getLogOperation());
    }

}
