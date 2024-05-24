package com.coroptis.index.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.FileReader;

@ExtendWith(MockitoExtension.class)
public class TypeDescriptorLoggedKeyTest {

    private final static TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
    private final static TypeDescriptor<String> tds = new TypeDescriptorString();

    @Test
    public void test_integer_read_write() throws Exception {
        final TypeDescriptorLoggedKey<Integer> tdlk = new TypeDescriptorLoggedKey<>(tdi);

        final LoggedKey<Integer> k1 = tdlk.getConvertorFromBytes()
                .fromBytes(tdlk.getConvertorToBytes().toBytes(LoggedKey.<Integer>of(LogOperation.POST, 87)));
        assertEquals(87, k1.getKey());
        assertEquals(LogOperation.POST, k1.getLogOperation());
    }

    @Test
    public void test_string_read_write() throws Exception {
        final TypeDescriptorLoggedKey<String> tdlk = new TypeDescriptorLoggedKey<>(tds);

        final LoggedKey<String> k1 = tdlk.getConvertorFromBytes()
                .fromBytes(tdlk.getConvertorToBytes().toBytes(LoggedKey.<String>of(LogOperation.POST, "aaa")));
        assertEquals("aaa", k1.getKey());
        assertEquals(LogOperation.POST, k1.getLogOperation());
    }

    @Test
    public void test_string_read_write_tombstone() throws Exception {
        final TypeDescriptorLoggedKey<String> tdlk = new TypeDescriptorLoggedKey<>(tds);

        final LoggedKey<String> k1 = tdlk.getConvertorFromBytes()
                .fromBytes(tdlk.getConvertorToBytes()
                        .toBytes(LoggedKey.<String>of(LogOperation.POST, TypeDescriptorString.TOMBSTONE_VALUE)));
        assertEquals(TypeDescriptorString.TOMBSTONE_VALUE, k1.getKey());
        assertEquals(LogOperation.POST, k1.getLogOperation());
    }

    @Mock
    private FileReader fileReader;

    @Test
    public void test_read_null() throws Exception {
        final TypeDescriptorLoggedKey<String> tdlk = new TypeDescriptorLoggedKey<>(tds);
        when(fileReader.read()).thenReturn(-1);
        final LoggedKey<String> k = tdlk.getTypeReader().read(fileReader);

        assertEquals(null, k);
    }

}
