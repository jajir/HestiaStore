package org.hestiastore.index.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.datatype.CompositeValue;
import org.hestiastore.index.datatype.TypeDescriptorComposite;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorUtf8String;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Demonstrates a UTF-8 string and long composite key in a high-level index.
 */
public class TypeDescriptorCompositeExampleIT {

    @Test
    void test_write_close_reopen_and_read_composite_key() {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<CompositeValue, String> configuration = IndexConfiguration
                .<CompositeValue, String>builder()
                .identity(identity -> identity.name("composite_key_example")
                        .keyClass(CompositeValue.class)
                        .valueClass(String.class)
                        .keyTypeDescriptor(
                                new Utf8StringLongKeyTypeDescriptor())
                        .valueTypeDescriptor(new TypeDescriptorUtf8String()))
                .build();
        final SegmentIndex<CompositeValue, String> index = SegmentIndex
                .create(directory, configuration);
        final CompositeValue key = CompositeValue.of("customer-🤩",
                42L);
        final String value = "UTF-8 value: příliš žluťoučký kůň";

        try (index) {
            index.put(key, value);
        }
        assertTrue(index.wasClosed());

        final SegmentIndex<CompositeValue, String> reopenedIndex = SegmentIndex
                .open(directory);
        try (reopenedIndex) {
            assertEquals(value, reopenedIndex.get(key));
        }
        assertTrue(reopenedIndex.wasClosed());
    }

    /**
     * Composite-key descriptor used by the example index.
     */
    public static final class Utf8StringLongKeyTypeDescriptor
            extends TypeDescriptorComposite {

        /**
         * Creates a descriptor for a UTF-8 string followed by a long.
         */
        public Utf8StringLongKeyTypeDescriptor() {
            super(List.of(new TypeDescriptorUtf8String(),
                    new TypeDescriptorLong()));
        }
    }
}
