package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.junit.jupiter.api.Test;

class DataTypeDescriptorRegistryTest {

    private static final String TD_FAULTY = MyFaultyTypeDescriptor.class
            .getName();

    @Test
    void test_integer_datatypeDescriptor() {
        final String tdInteger = DataTypeDescriptorRegistry
                .getTypeDescriptor(Integer.class);

        assertNotNull(tdInteger);
        assertEquals("org.hestiastore.index.datatype.TypeDescriptorInteger",
                tdInteger);
    }

    @Test
    void test_makeInstance_TypeDescriptorString() {
        final TypeDescriptor<String> ss = DataTypeDescriptorRegistry
                .makeInstance(
                        "org.hestiastore.index.datatype.TypeDescriptorString");

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
                () -> DataTypeDescriptorRegistry.makeInstance(TD_FAULTY));

        assertEquals(
                "In class 'org.hestiastore.index.segmentindex.DataTypeDescriptorRegistryTest$MyFaultyTypeDescriptor'"
                        + " there is no public default (no-args) costructor.",
                e.getMessage());
    }

    class MyFaultyTypeDescriptor extends TypeDescriptorShortString {

        MyFaultyTypeDescriptor(final String name) {
            // super faulty constructor
        }

    }

}
