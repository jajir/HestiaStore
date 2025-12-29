package org.coroptis.index.it;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.junit.jupiter.api.Test;

public class ExampleIT {

    @Test
    void test_create_1() {
        // Create an in-memory file system abstraction
        final Directory directory = new MemDirectory();

        // Prepare index configuration
        final IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("test_index") //
                .build();

        // create new index
        SegmentIndex<String, String> index = SegmentIndex
                .<String, String>create(directory, conf);

        // Do some work with the index
        index.put("Hello", "World");

        String value = index.get("Hello");
        System.out.println("Value for 'Hello': " + value);

        index.close();

        reopen(directory);
    }

    private void reopen(final Directory directory) {
        IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("test_index") //
                .build();

        SegmentIndex<String, String> index = SegmentIndex
                .<String, String>open(directory, conf);

        index.getStream().forEach(entry -> {
            System.out.println("Entry: " + entry);
        });

        SegmentWindow window = SegmentWindow.of(1000, 10);

        index.getStream(window).forEach(entry -> {
            System.out.println("Entry: " + entry);
        });

        index.flush();

        index.checkAndRepairConsistency();

        index.compact();
    }

}
