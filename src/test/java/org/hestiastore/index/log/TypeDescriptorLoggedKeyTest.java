package org.hestiastore.index.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.FileReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TypeDescriptorLoggedKeyTest {

    private static final TypeDescriptor<Integer> TDI = new TypeDescriptorInteger();
    private static final TypeDescriptor<String> TDS = new TypeDescriptorShortString();

    @Test
    void test_integer_read_write() {
        final TypeDescriptorLoggedKey<Integer> tdlk = new TypeDescriptorLoggedKey<>(
                TDI);

        final LoggedKey<Integer> k1 = tdlk.getConvertorFromBytes()
                .fromBytes(tdlk.getConvertorToBytes()
                        .toBytes(LoggedKey.<Integer>of(LogOperation.POST, 87)));
        assertEquals(87, k1.getKey());
        assertEquals(LogOperation.POST, k1.getLogOperation());
    }

    @Test
    void test_string_read_write() {
        final TypeDescriptorLoggedKey<String> tdlk = new TypeDescriptorLoggedKey<>(
                TDS);

        final LoggedKey<String> k1 = tdlk.getConvertorFromBytes()
                .fromBytes(tdlk.getConvertorToBytes().toBytes(
                        LoggedKey.<String>of(LogOperation.POST, "aaa")));
        assertEquals("aaa", k1.getKey());
        assertEquals(LogOperation.POST, k1.getLogOperation());
    }

    @Test
    void test_string_read_write_tombstone() {
        final TypeDescriptorLoggedKey<String> tdlk = new TypeDescriptorLoggedKey<>(
                TDS);

        final LoggedKey<String> k1 = tdlk.getConvertorFromBytes()
                .fromBytes(tdlk.getConvertorToBytes()
                        .toBytes(LoggedKey.<String>of(LogOperation.POST,
                                TypeDescriptorShortString.TOMBSTONE_VALUE)));
        assertEquals(TypeDescriptorShortString.TOMBSTONE_VALUE, k1.getKey());
        assertEquals(LogOperation.POST, k1.getLogOperation());
    }

    @Mock
    private FileReader fileReader;

    @Test
    void test_read_null() {
        final TypeDescriptorLoggedKey<String> tdlk = new TypeDescriptorLoggedKey<>(
                TDS);
        when(fileReader.read()).thenReturn(-1);
        final LoggedKey<String> k = tdlk.getTypeReader().read(fileReader);

        assertEquals(null, k);
    }

}
