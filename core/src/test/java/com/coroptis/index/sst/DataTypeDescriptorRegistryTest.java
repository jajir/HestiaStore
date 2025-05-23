package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.coroptis.index.IndexException;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorString;

public class DataTypeDescriptorRegistryTest {

    @Test
    void test_integer_datatypeDescriptor() {
        final String tdInteger = DataTypeDescriptorRegistry
                .getTypeDescriptor(Integer.class);

        assertNotNull(tdInteger);
        assertEquals("com.coroptis.index.datatype.TypeDescriptorInteger",
                tdInteger);
    }

    @Test
    void test_makeInstance_TypeDescriptorString() {
        final TypeDescriptor<String> ss = DataTypeDescriptorRegistry
                .makeInstance(
                        "com.coroptis.index.datatype.TypeDescriptorString");

        assertNotNull(ss);
        assertNotNull(ss.getConvertorFromBytes());
    }

    @Test
    void test_makeInstance_invalidClassNameString() {
        final IndexException e = assertThrows(IndexException.class,
                () -> DataTypeDescriptorRegistry.makeInstance("brekeek"));

        assertEquals(
                "Unable to find class 'brekeek'. "
                        + "Make sure the class is in the classpath.",
                e.getMessage());
    }

    @Test
    void test_makeInstance_classDoesntHaveDefaultConstructor() {
        final IndexException e = assertThrows(IndexException.class,
                () -> DataTypeDescriptorRegistry
                        .makeInstance("java.lang.String"));

        assertEquals(
                "Class 'java.lang.String' does not implement TypeDescriptor",
                e.getMessage());
    }

    @Test
    void test_makeInstance_classIsNotTypeDescriptor() {
        final IndexException e = assertThrows(IndexException.class,
                () -> DataTypeDescriptorRegistry
                        .makeInstance(MyFaultyTypeDescriptor.class.getName()));

        assertEquals(
                "In class 'com.coroptis.index.sst.DataTypeDescriptorRegistryTest$MyFaultyTypeDescriptor'"
                        + " there is no public default (no-args) costructor.",
                e.getMessage());
    }

    class MyFaultyTypeDescriptor extends TypeDescriptorString {

        MyFaultyTypeDescriptor(final String name) {
            // super faulty constructor;
        }

    }

}
