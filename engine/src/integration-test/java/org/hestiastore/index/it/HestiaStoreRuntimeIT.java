package org.hestiastore.index.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.HestiaStoreRuntime;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.junit.jupiter.api.Test;

class HestiaStoreRuntimeIT {

    @Test
    void sharedRuntimeKeepsBothIndexesDurableAfterClose() {
        final Directory firstDirectory = new MemDirectory();
        final Directory secondDirectory = new MemDirectory();

        try (HestiaStoreRuntime runtime = HestiaStoreRuntime.builder()
                .segmentMaintenanceThreads(1)
                .splitMaintenanceThreads(1)
                .build()) {
            try (SegmentIndex<Integer, String> firstIndex = SegmentIndex
                    .create(firstDirectory, configuration("shared-runtime-a"),
                            runtime);
                    SegmentIndex<Integer, String> secondIndex = SegmentIndex
                            .create(secondDirectory,
                                    configuration("shared-runtime-b"),
                                    runtime)) {
                firstIndex.put(1, "first-one");
                firstIndex.put(2, "first-two");
                secondIndex.put(1, "second-one");
                secondIndex.put(3, "second-three");
            }
        }

        try (SegmentIndex<Integer, String> firstIndex = SegmentIndex
                .open(firstDirectory);
                SegmentIndex<Integer, String> secondIndex = SegmentIndex
                        .open(secondDirectory)) {
            assertEquals("first-one", firstIndex.get(1));
            assertEquals("first-two", firstIndex.get(2));
            assertEquals("second-one", secondIndex.get(1));
            assertEquals("second-three", secondIndex.get(3));
        }
    }

    private static IndexConfiguration<Integer, String> configuration(
            final String indexName) {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity
                        .keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity
                        .valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name(indexName))
                .logging(logging -> logging.contextEnabled(false))
                .maintenance(maintenance -> maintenance.indexThreads(1))
                .maintenance(maintenance -> maintenance
                        .registryLifecycleThreads(1))
                .build();
    }
}
