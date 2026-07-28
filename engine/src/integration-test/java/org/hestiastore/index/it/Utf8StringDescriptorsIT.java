package org.hestiastore.index.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hestiastore.index.datatype.TypeDescriptorTinyUtf8String;
import org.hestiastore.index.datatype.TypeDescriptorUtf8String;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.junit.jupiter.api.Test;

class Utf8StringDescriptorsIT {

    @Test
    void persistedConfigurationReopensAndReadsMultilingualEntry() {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<String, String> configuration = IndexConfiguration
                .<String, String>builder()
                .identity(identity -> identity.name("utf8-index")
                        .keyClass(String.class).valueClass(String.class)
                        .keyTypeDescriptor(
                                new TypeDescriptorTinyUtf8String())
                        .valueTypeDescriptor(
                                new TypeDescriptorUtf8String()))
                .build();
        final String key = "objednávka-🙂";
        final String value = "Čeština Ελληνικά Кириллица 日本語 👨‍👩‍👧‍👦";

        try (SegmentIndex<String, String> index = SegmentIndex.create(directory,
                configuration)) {
            index.put(key, value);
            index.maintenance().flush();
        }

        try (SegmentIndex<String, String> reopened = SegmentIndex
                .open(directory)) {
            assertEquals(value, reopened.get(key));
        }
    }
}
